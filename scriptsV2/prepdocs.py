import argparse
import base64
import glob
import html
import io
import os
import re
import tempfile
import time
from dotenv import load_dotenv

import openai
import tiktoken
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential, AzureNamedKeyCredential
from azure.identity import AzureDeveloperCliCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswParameters,
    PrioritizedFields,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SemanticConfiguration,
    SemanticField,
    SemanticSettings,
    SimpleField,
    VectorSearch,
    VectorSearchAlgorithmConfiguration,
)
from azure.storage.blob import BlobServiceClient
from pypdf import PdfReader, PdfWriter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

args = argparse.Namespace(
    verbose=False,
    openaihost="azure",
    datalakestorageaccount=None,
    datalakefilesystem=None,
    datalakepath=None,
    remove=False,
    useacls=False,
    skipblobs=False,
    storageaccount=None,
    container=None,
)
adls_gen2_creds = None
storage_creds = None

MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100
SECTION_OVERLAP = 100

open_ai_token_cache = {}
CACHE_KEY_TOKEN_CRED = "openai_token_cred"
CACHE_KEY_CREATED_TIME = "created_time"
CACHE_KEY_TOKEN_TYPE = "token_type"

# Embedding batch support section
SUPPORTED_BATCH_AOAI_MODEL = {"text-embedding-ada-002": {"token_limit": 8100, "max_batch_size": 16}}


def calculate_tokens_emb_aoai(input: str):
    encoding = tiktoken.encoding_for_model(openaimodelname)
    return len(encoding.encode(input))


def blob_name_from_file_page(filename, page=0):
    if os.path.splitext(filename)[1].lower() == ".pdf":
        return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"
    else:
        return os.path.basename(filename)


def upload_blobs(filename):
    blob_service = BlobServiceClient(
        account_url=f"https://{storageaccount}.blob.core.windows.net", credential=storage_creds
    )
    blob_container = blob_service.get_container_client(container)
    if not blob_container.exists():
        blob_container.create_container()

    # if file is PDF split into pages and upload each page as a separate blob
    if os.path.splitext(filename)[1].lower() == ".pdf":
        reader = PdfReader(filename)
        pages = reader.pages
        for i in range(len(pages)):
            blob_name = blob_name_from_file_page(filename, i)
            print(f"\tUploading blob for page {i} -> {blob_name}")
            f = io.BytesIO()
            writer = PdfWriter()
            writer.add_page(pages[i])
            writer.write(f)
            f.seek(0)
            blob_container.upload_blob(blob_name, f, overwrite=True)
    else:
        blob_name = blob_name_from_file_page(filename)
        with open(filename, "rb") as data:
            blob_container.upload_blob(blob_name, data, overwrite=True)


def remove_blobs(filename):
    print(f"Removing blobs for '{filename or '<all>'}'")
    blob_service = BlobServiceClient(
        account_url=f"https://{storageaccount}.blob.core.windows.net", credential=storage_creds
    )
    blob_container = blob_service.get_container_client(container)
    if blob_container.exists():
        if filename is None:
            blobs = blob_container.list_blob_names()
        else:
            prefix = os.path.splitext(os.path.basename(filename))[0]
            blobs = filter(
                lambda b: re.match(f"{prefix}-\d+\.pdf", b),
                blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]),
            )
        for b in blobs:
            if verbose:
                print(f"\tRemoving blob {b}")
            blob_container.delete_blob(b)


def table_to_html(table):
    table_html = "<table>"
    rows = [
        sorted([cell for cell in table.cells if cell.row_index == i], key=lambda cell: cell.column_index)
        for i in range(table.row_count)
    ]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag = "th" if (cell.kind == "columnHeader" or cell.kind == "rowHeader") else "td"
            cell_spans = ""
            if cell.column_span > 1:
                cell_spans += f" colSpan={cell.column_span}"
            if cell.row_span > 1:
                cell_spans += f" rowSpan={cell.row_span}"
            table_html += f"<{tag}{cell_spans}>{html.escape(cell.content)}</{tag}>"
        table_html += "</tr>"
    table_html += "</table>"
    return table_html


def get_document_text(filename):
    offset = 0
    page_map = []

    if verbose:
        print(f"Extracting text from '{filename}' using Azure Form Recognizer")
    form_recognizer_client = DocumentAnalysisClient(
        endpoint=f"https://{formrecognizerservice}.cognitiveservices.azure.com/",
        credential=formrecognizer_creds,
        headers={"x-ms-useragent": "azure-search-chat-demo/1.0.0"},
    )
    with open(filename, "rb") as f:
        poller = form_recognizer_client.begin_analyze_document("prebuilt-layout", document=f)
    form_recognizer_results = poller.result()

    for page_num, page in enumerate(form_recognizer_results.pages):
        tables_on_page = [
            table
            for table in form_recognizer_results.tables
            if table.bounding_regions[0].page_number == page_num + 1
        ]

        # mark all positions of the table spans in the page
        page_offset = page.spans[0].offset
        page_length = page.spans[0].length
        table_chars = [-1] * page_length
        for table_id, table in enumerate(tables_on_page):
            for span in table.spans:
                # replace all table spans with "table_id" in table_chars array
                for i in range(span.length):
                    idx = span.offset - page_offset + i
                    if idx >= 0 and idx < page_length:
                        table_chars[idx] = table_id

        # build page text by replacing characters in table spans with table html
        page_text = ""
        added_tables = set()
        for idx, table_id in enumerate(table_chars):
            if table_id == -1:
                page_text += form_recognizer_results.content[page_offset + idx]
            elif table_id not in added_tables:
                page_text += table_to_html(tables_on_page[table_id])
                added_tables.add(table_id)

        page_text += " "
        page_map.append((page_num, offset, page_text))
        offset += len(page_text)

    return page_map


def split_text(page_map, filename):
    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]
    if verbose:
        print(f"Splitting '{filename}' into sections")

    def find_page(offset):
        num_pages = len(page_map)
        for i in range(num_pages - 1):
            if offset >= page_map[i][1] and offset < page_map[i + 1][1]:
                return i
        return num_pages - 1

    all_text = "".join(p[2] for p in page_map)
    length = len(all_text)
    start = 0
    end = length
    while start + SECTION_OVERLAP < length:
        last_word = -1
        end = start + MAX_SECTION_LENGTH

        if end > length:
            end = length
        else:
            # Try to find the end of the sentence
            while (
                end < length
                and (end - start - MAX_SECTION_LENGTH) < SENTENCE_SEARCH_LIMIT
                and all_text[end] not in SENTENCE_ENDINGS
            ):
                if all_text[end] in WORDS_BREAKS:
                    last_word = end
                end += 1
            if end < length and all_text[end] not in SENTENCE_ENDINGS and last_word > 0:
                end = last_word  # Fall back to at least keeping a whole word
        if end < length:
            end += 1

        # Try to find the start of the sentence or at least a whole word boundary
        last_word = -1
        while (
            start > 0
            and start > end - MAX_SECTION_LENGTH - 2 * SENTENCE_SEARCH_LIMIT
            and all_text[start] not in SENTENCE_ENDINGS
        ):
            if all_text[start] in WORDS_BREAKS:
                last_word = start
            start -= 1
        if all_text[start] not in SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        if start > 0:
            start += 1

        section_text = all_text[start:end]
        yield (section_text, find_page(start))

        last_table_start = section_text.rfind("<table")
        if last_table_start > 2 * SENTENCE_SEARCH_LIMIT and last_table_start > section_text.rfind("</table"):
            # If the section ends with an unclosed table, we need to start the next section with the table.
            # If table starts inside SENTENCE_SEARCH_LIMIT, we ignore it, as that will cause an infinite loop for tables longer than MAX_SECTION_LENGTH
            # If last table starts inside SECTION_OVERLAP, keep overlapping
            if verbose:
                print(
                    f"Section ends with unclosed table, starting next section with the table at page {find_page(start)} offset {start} table start {last_table_start}"
                )
            start = min(end - SECTION_OVERLAP, start + last_table_start)
        else:
            start = end - SECTION_OVERLAP

    if start + SECTION_OVERLAP < end:
        yield (all_text[start:end], find_page(start))


def filename_to_id(filename):
    filename_ascii = re.sub("[^0-9a-zA-Z_-]", "_", filename)
    filename_hash = base64.b16encode(filename.encode("utf-8")).decode("ascii")
    return f"file-{filename_ascii}-{filename_hash}"


def create_sections(filename, page_map, use_vectors, embedding_deployment: str = None, embedding_model: str = None):
    file_id = filename_to_id(filename)
    for i, (content, pagenum) in enumerate(split_text(page_map, filename)):
        filepath = blob_name_from_file_page(filename, pagenum)
        url = "https://{0}.blob.core.windows.net/{1}/{2}".format(storageaccount, container, filepath)
        section = {
            "id": f"{file_id}-page-{i}",
            "content": content,
            "category": category,
            "filepath": filepath,
            "title": filename,
            "url": url
        }
        section["contentVector"] = compute_embedding(content, embedding_deployment, embedding_model)
        yield section


def before_retry_sleep(retry_state):
    if verbose:
        print("Rate limited on the OpenAI embeddings API, sleeping before retrying...")


@retry(
    retry=retry_if_exception_type(openai.error.RateLimitError),
    wait=wait_random_exponential(min=15, max=60),
    stop=stop_after_attempt(15),
    before_sleep=before_retry_sleep,
)
def compute_embedding(text, embedding_deployment, embedding_model):
    refresh_openai_token()
    embedding_args = {"deployment_id": embedding_deployment} if openaihost != "openai" else {}
    return openai.Embedding.create(**embedding_args, model=embedding_model, input=text)["data"][0]["embedding"]


@retry(
    retry=retry_if_exception_type(openai.error.RateLimitError),
    wait=wait_random_exponential(min=15, max=60),
    stop=stop_after_attempt(15),
    before_sleep=before_retry_sleep,
)
def compute_embedding_in_batch(texts):
    refresh_openai_token()
    embedding_args = {"deployment_id": openaideployment} if openaihost != "openai" else {}
    emb_response = openai.Embedding.create(**embedding_args, model=openaimodelname, input=texts)
    return [data.embedding for data in emb_response.data]


def create_search_index():
    global index, search_creds, searchservice 
    if verbose:
        print(f"Ensuring search index {index} exists")
    index_client = SearchIndexClient(
        endpoint=f"https://{searchservice}.search.windows.net/", credential=search_creds
    )
    fields = [
        SimpleField(name="id", type="Edm.String", key=True),
        SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
        SearchableField(name="title", type="Edm.String", analyzer_name="en.microsoft"),
        SearchField(
            name="contentVector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            hidden=False,
            searchable=True,
            filterable=False,
            sortable=False,
            facetable=False,
            vector_search_dimensions=1536,
            vector_search_configuration="default",
        ),
        SimpleField(name="category", type="Edm.String", filterable=True, facetable=True),
        SimpleField(name="metadata", type="Edm.String", filterable=True, facetable=True),
        SimpleField(name="filepath", type="Edm.String", filterable=True, facetable=True),
        SearchableField(name="url", type="Edm.String")
    ]
  
    if args.useacls:
        fields.append(
            SimpleField(name="oids", type=SearchFieldDataType.Collection(SearchFieldDataType.String), filterable=True)
        )
        fields.append(
            SimpleField(name="groups", type=SearchFieldDataType.Collection(SearchFieldDataType.String), filterable=True)
        )

    if index not in index_client.list_index_names():
        index = SearchIndex(
            name=index,
            fields=fields,
            semantic_settings=SemanticSettings(
                configurations=[
                    SemanticConfiguration(
                        name="default",
                        prioritized_fields=PrioritizedFields(
                            title_field=None, prioritized_content_fields=[SemanticField(field_name="content")]
                        ),
                    )
                ]
            ),
            vector_search=VectorSearch(
                algorithm_configurations=[
                    VectorSearchAlgorithmConfiguration(
                        name="default", kind="hnsw", hnsw_parameters=HnswParameters(metric="cosine")
                    )
                ]
            ),
        )
        if verbose:
            print(f"Creating {index} search index")
        index_client.create_index(index)
    else:
        if verbose:
            print(f"Search index {index} already exists")


def update_embeddings_in_batch(sections):
    batch_queue = []
    copy_s = []
    batch_response = {}
    token_count = 0
    for s in sections:
        token_count += calculate_tokens_emb_aoai(s["content"])
        if (
            token_count <= SUPPORTED_BATCH_AOAI_MODEL[openaimodelname]["token_limit"]
            and len(batch_queue) < SUPPORTED_BATCH_AOAI_MODEL[openaimodelname]["max_batch_size"]
        ):
            batch_queue.append(s)
            copy_s.append(s)
        else:
            emb_responses = compute_embedding_in_batch([item["content"] for item in batch_queue])
            if verbose:
                print(f"Batch Completed. Batch size  {len(batch_queue)} Token count {token_count}")
            for emb, item in zip(emb_responses, batch_queue):
                batch_response[item["id"]] = emb
            batch_queue = []
            batch_queue.append(s)
            token_count = calculate_tokens_emb_aoai(s["content"])

    if batch_queue:
        emb_responses = compute_embedding_in_batch([item["content"] for item in batch_queue])
        if verbose:
            print(f"Batch Completed. Batch size  {len(batch_queue)} Token count {token_count}")
        for emb, item in zip(emb_responses, batch_queue):
            batch_response[item["id"]] = emb

    for s in copy_s:
        s["contentVector"] = batch_response[s["id"]]
        yield s


def index_sections(filename, sections, acls=None):
    if verbose:
        print(f"Indexing sections from '{filename}' into search index '{index}'")
    search_client = SearchClient(
        endpoint=f"https://{searchservice}.search.windows.net/", index_name=index, credential=search_creds
    )
    i = 0
    batch = []
    for s in sections:
        if acls:
            s.update(acls)
        batch.append(s)
        i += 1
        if i % 1000 == 0:
            results = search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if verbose:
                print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
            batch = []

    if len(batch) > 0:
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
        if verbose:
            print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")


def remove_from_index(filename):
    if verbose:
        print(f"Removing sections from '{filename or '<all>'}' from search index '{index}'")
    search_client = SearchClient(
        endpoint=f"https://{searchservice}.search.windows.net/", index_name=index, credential=search_creds
    )
    while True:
        filter = None if filename is None else f"sourcefile eq '{os.path.basename(filename)}'"
        r = search_client.search("", filter=filter, top=1000, include_total_count=True)
        if r.get_count() == 0:
            break
        r = search_client.delete_documents(documents=[{"id": d["id"]} for d in r])
        if verbose:
            print(f"\tRemoved {len(r)} sections from index")
        # It can take a few seconds for search results to reflect changes, so wait a bit
        time.sleep(2)


def refresh_openai_token():
    """
    Refresh OpenAI token every 5 minutes
    """
    if (
        CACHE_KEY_TOKEN_TYPE in open_ai_token_cache
        and open_ai_token_cache[CACHE_KEY_TOKEN_TYPE] == "azure_ad"
        and open_ai_token_cache[CACHE_KEY_CREATED_TIME] + 300 < time.time()
    ):
        token_cred = open_ai_token_cache[CACHE_KEY_TOKEN_CRED]
        openai.api_key = token_cred.get_token("https://cognitiveservices.azure.com/.default").token
        open_ai_token_cache[CACHE_KEY_CREATED_TIME] = time.time()


def read_files(
    path_pattern: str,
    use_vectors: bool,
    vectors_batch_support: bool,
    embedding_deployment: str = None,
    embedding_model: str = None,
):
    """
    Recursively read directory structure under `path_pattern`
    and execute indexing for the individual files
    """
    for filename in sorted(glob.glob(path_pattern), reverse=True):
        try:
            if verbose:
                print(f"Processing '{filename}'")

            if os.path.isdir(filename):
                read_files(filename + "/*", use_vectors, vectors_batch_support, embedding_deployment, embedding_model)
                continue

            upload_blobs(filename)
            page_map = get_document_text(filename)

            sections = create_sections(
                os.path.basename(filename),
                page_map,
                use_vectors and not vectors_batch_support,
                embedding_deployment,
                embedding_model,
            )
            if use_vectors and vectors_batch_support:
                sections = update_embeddings_in_batch(sections)
            index_sections(os.path.basename(filename), sections)
            file_n = str(filename).split("\\")[-1]
            folder = filename[:-(len(file_n)+1)]
            os.rename(filename, "{0}_archive/{1}".format(folder, file_n))
        except:
            continue

        
def read_adls_gen2_files(
    use_vectors: bool, vectors_batch_support: bool, embedding_deployment: str = None, embedding_model: str = None
):
    datalake_service = DataLakeServiceClient(
        account_url=f"https://{args.datalakestorageaccount}.dfs.core.windows.net", credential=adls_gen2_creds
    )
    filesystem_client = datalake_service.get_file_system_client(file_system=args.datalakefilesystem)
    paths = filesystem_client.get_paths(path=args.datalakepath, recursive=True)
    for path in paths:
        if not path.is_directory:
            if args.remove:
                remove_blobs(path.name)
                remove_from_index(path.name)
            else:
                temp_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(path.name))
                try:
                    temp_file = open(temp_file_path, "wb")
                    file_client = filesystem_client.get_file_client(path)
                    file_client.download_file().readinto(temp_file)

                    acls = None
                    if args.useacls:
                        # Parse out user ids and group ids
                        acls = {"oids": [], "groups": []}
                        # https://learn.microsoft.com/python/api/azure-storage-file-datalake/azure.storage.filedatalake.datalakefileclient?view=azure-python#azure-storage-filedatalake-datalakefileclient-get-access-control
                        # Request ACLs as GUIDs
                        acl_list = file_client.get_access_control(upn=False)["acl"]
                        # https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-access-control
                        # ACL Format: user::rwx,group::r-x,other::r--,user:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:r--
                        acl_list = acl_list.split(",")
                        for acl in acl_list:
                            acl_parts = acl.split(":")
                            if len(acl_parts) != 3:
                                continue
                            if len(acl_parts[1]) == 0:
                                continue
                            if acl_parts[0] == "user" and "r" in acl_parts[2]:
                                acls["oids"].append(acl_parts[1])
                            if acl_parts[0] == "group" and "r" in acl_parts[2]:
                                acls["groups"].append(acl_parts[1])

                    if not args.skipblobs:
                        upload_blobs(temp_file.name)
                    page_map = get_document_text(temp_file.name)
                    sections = create_sections(
                        os.path.basename(path.name),
                        page_map,
                        use_vectors and not vectors_batch_support,
                        embedding_deployment,
                        embedding_model,
                    )
                    if use_vectors and vectors_batch_support:
                        sections = update_embeddings_in_batch(sections)
                    index_sections(os.path.basename(path.name), sections, acls)
                except Exception as e:
                    print(f"\tGot an error while reading {path.name} -> {e} --> skipping file")
                finally:
                    try:
                        temp_file.close()
                        os.remove(temp_file_path)
                    except Exception as e:
                        print(f"\tGot an error while deleting {temp_file_path} -> {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare documents by extracting content from PDFs, splitting content into sections, uploading to blob storage, and indexing in a search index.",
        epilog="Example: prepdocs.py '..\data\*' --storageaccount myaccount --container mycontainer --searchservice mysearch --index myindex -v",
    )
    parser.add_argument(
        "--env",
        required=False,
        help="Optional. Use this to define the Azure directory where to authenticate)",
    )

    args = parser.parse_args()

    load_dotenv(".azure/{0}/.env".format(args.env), override=True)
    files = "{0}/../data/{1}/*".format(os.getcwd(), args.env)
    storageaccount = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_STORAGE_CONTAINER")
    storagekey = os.getenv("AZURE_STORAGE_KEY")

    searchservice =  os.getenv("AZURE_SEARCH_SERVICE")
    searchkey = os.getenv("AZURE_SEARCH_KEY")
    openaihost = "azure"
    openaiservice = os.getenv("AZURE_OPENAI_RESOURCE")
    openaikey = os.getenv("AZURE_OPENAI_KEY")
    openaimodelname = "text-embedding-ada-002"
    openaideployment = "embedding"
    index = os.getenv("AZURE_SEARCH_INDEX")
    formrecognizerservice = os.getenv("AZURE_FORMRECOGNIZER_SERVICE")
    formrecognizerkey = os.getenv("AZURE_FORMRECOGNIZER_KEY")

    verbose = True
    category = "default"

    parser.add_argument(
        "--datalakestorageaccount", required=False, help="Optional. Azure Data Lake Storage Gen2 Account name"
    )
    parser.add_argument(
        "--datalakefilesystem",
        required=False,
        default="gptkbcontainer",
        help="Optional. Azure Data Lake Storage Gen2 filesystem name",
    )
    parser.add_argument(
        "--datalakepath",
        required=False,
        help="Optional. Azure Data Lake Storage Gen2 filesystem path containing files to index. If omitted, index the entire filesystem",
    )
    parser.add_argument(
        "--datalakekey", required=False, help="Optional. Use this key when authenticating to Azure Data Lake Gen2"
    )
    parser.add_argument(
        "--useacls", action="store_true", help="Store ACLs from Azure Data Lake Gen2 Filesystem in the search index"
    )
    parser.add_argument(
        "--category", help="Value for the category field in the search index for all sections indexed in this run"
    )
    parser.add_argument(
        "--skipblobs", action="store_true", help="Skip uploading individual pages to Azure Blob Storage"
    )

    args = parser.parse_args()

    # Use the current user identity to connect to Azure services unless a key is explicitly set for any of them
#    adls_gen2_creds = azd_credential if args.datalakekey is None else AzureKeyCredential(args.datalakekey)
    search_creds = AzureKeyCredential(searchkey)
    use_vectors = True
    compute_vectors_in_batch = openaimodelname in SUPPORTED_BATCH_AOAI_MODEL

    storage_creds = AzureNamedKeyCredential(storageaccount, storagekey)

    if formrecognizerservice is None:
        print(
            "Error: Azure Form Recognizer service is not provided. Please provide formrecognizerservice or use --localpdfparser for local pypdf parser."
        )
        exit(1)
    formrecognizer_creds = AzureKeyCredential(formrecognizerkey)

    if use_vectors:
        openai.api_key = openaikey
        openai.api_type = "azure"
        openai.api_base = f"https://{openaiservice}.openai.azure.com"
        openai.api_version = "2023-05-15"



    create_search_index()

    print("Processing files...")
    if not args.datalakestorageaccount:
        print(f"Using local files in {files}")
        read_files(files, use_vectors, compute_vectors_in_batch, openaideployment, openaimodelname)
    else:
        print(f"Using Data Lake Gen2 Storage Account {args.datalakestorageaccount}")
        read_adls_gen2_files(use_vectors, compute_vectors_in_batch, args.openaideployment, args.openaimodelname)
