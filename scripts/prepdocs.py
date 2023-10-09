from dotenv import load_dotenv
import os
import argparse

import dataclasses
import time

from tqdm import tqdm
from azure.identity import AzureDeveloperCliCredential
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SemanticField,
    SemanticSettings,
    SemanticConfiguration,
    SearchIndex,
    PrioritizedFields,
    VectorSearch,
    VectorSearchAlgorithmConfiguration,
    HnswParameters
)
from azure.search.documents import SearchClient
from azure.ai.formrecognizer import DocumentAnalysisClient


from data_utils import chunk_directory


def create_search_index(index_name, index_client):
    print(f"Ensuring search index {index_name} exists")
    if index_name not in index_client.list_index_names():
        index = SearchIndex(
            name=index_name,
            fields=[
                SearchableField(name="id", type="Edm.String", key=True),
                SearchableField(
                    name="content", type="Edm.String", analyzer_name="en.lucene"
                ),
                SearchableField(
                    name="title", type="Edm.String", analyzer_name="en.lucene"
                ),
                SearchableField(name="filepath", type="Edm.String"),
                SearchableField(name="url", type="Edm.String"),
                SearchableField(name="metadata", type="Edm.String"),
                SearchField(name="contentVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                            hidden=False, searchable=True, filterable=False, sortable=False, facetable=False,
                            vector_search_dimensions=1536, vector_search_configuration="default"),
            ],
            semantic_settings=SemanticSettings(
                configurations=[
                    SemanticConfiguration(
                        name="default",
                        prioritized_fields=PrioritizedFields(
                            title_field=SemanticField(field_name="title"),
                            prioritized_content_fields=[
                                SemanticField(field_name="content")
                            ],
                        ),
                    )
                ]
            ),
            vector_search=VectorSearch(
                algorithm_configurations=[
                    VectorSearchAlgorithmConfiguration(
                        name="default",
                        kind="hnsw",
                        hnsw_parameters=HnswParameters(metric="cosine")
                    )
                ]
            )
        )
        print(f"Creating {index_name} search index")
        index_client.create_index(index)
    else:
        print(f"Search index {index_name} already exists")


def upload_documents_to_index(docs, search_client, upload_batch_size=50):
    to_upload_dicts = []

    id = 0
    for document in docs:
        d = dataclasses.asdict(document)
        # add id to documents
        d.update({"@search.action": "upload", "id": str(id)})
        if "contentVector" in d and d["contentVector"] is None:
            del d["contentVector"]
        to_upload_dicts.append(d)
        id += 1

    # Upload the documents in batches of upload_batch_size
    for i in tqdm(
        range(0, len(to_upload_dicts), upload_batch_size), desc="Indexing Chunks..."
    ):
        batch = to_upload_dicts[i : i + upload_batch_size]
        results = search_client.upload_documents(documents=batch)
        num_failures = 0
        errors = set()
        for result in results:
            if not result.succeeded:
                print(
                    f"Indexing Failed for {result.key} with ERROR: {result.error_message}"
                )
                num_failures += 1
                errors.add(result.error_message)
        if num_failures > 0:
            raise Exception(
                f"INDEXING FAILED for {num_failures} documents. Please recreate the index."
                f"To Debug: PLEASE CHECK chunk_size and upload_batch_size. \n Error Messages: {list(errors)}"
            )


def validate_index(index_name, index_client):
    for retry_count in range(5):
        stats = index_client.get_index_statistics(index_name)
        num_chunks = stats["document_count"]
        if num_chunks == 0 and retry_count < 4:
            print("Index is empty. Waiting 60 seconds to check again...")
            time.sleep(60)
        elif num_chunks == 0 and retry_count == 4:
            print("Index is empty. Please investigate and re-index.")
        else:
            print(f"The index contains {num_chunks} chunks.")
            average_chunk_size = stats["storage_size"] / num_chunks
            print(f"The average chunk size of the index is {average_chunk_size} bytes.")
            break


def create_and_populate_index(
    index_name, index_client, search_client, form_recognizer_client, embeddingkey, embedding_endpoint
):
    # create or update search index with compatible schema
    create_search_index(index_name, index_client)

    # chunk directory
    print("Chunking directory...")
    result = chunk_directory(
        "./data/{0}".format(os.getenv("AZURE_ENV_NAME")),
        form_recognizer_client=form_recognizer_client,
        use_layout=True,
        ignore_errors=False,
        njobs=1,
        add_embeddings=True,
        embeddingkey=embeddingkey,
        embedding_endpoint=embedding_endpoint
    )

    if len(result.chunks) == 0:
        raise Exception("No chunks found. Please check the data path and chunk size.")

    print(f"Processed {result.total_files} files")
    print(f"Unsupported formats: {result.num_unsupported_format_files} files")
    print(f"Files with errors: {result.num_files_with_errors} files")
    print(f"Found {len(result.chunks)} chunks")

    # upload documents to index
    print("Uploading documents to index...")
    upload_documents_to_index(result.chunks, search_client)

    # check if index is ready/validate index
    print("Validating index...")
    validate_index(index_name, index_client)
    print("Index validation completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare documents by extracting content from PDFs, splitting content into sections and indexing in a search index.",
        epilog="Example: prepdocs.py --searchservice mysearch --index myindex",
    )

    parser.add_argument(
        "--env",
        required=False,
        help="Optional. Use this to define the Azure directory where to authenticate)",
    )

    args = parser.parse_args()

    load_dotenv(".azure/{0}/.env".format(args.env), override=True)

    searchservice = os.getenv("AZURE_SEARCH_SERVICE")
    searchindex = os.getenv("AZURE_SEARCH_INDEX")
    search_creds = AzureKeyCredential(os.getenv("AZURE_SEARCH_KEY"))

    formrecognizerservice = os.getenv("AZURE_FORMRECOGNIZER_SERVICE")
    formrecognizer_creds = AzureKeyCredential(os.getenv("AZURE_FORMRECOGNIZER_KEY"))

    embeddingendpoint = os.getenv("AZURE_OPENAI_EMBEDDING_ENDPOINT")
    embeddingkey = AzureKeyCredential(os.getenv("AZURE_OPENAI_EMBEDDING_KEY"))

    print("Data preparation script started")
    print("Preparing data for index:", searchindex)
    search_endpoint = f"https://{searchservice}.search.windows.net/"
    index_client = SearchIndexClient(endpoint=search_endpoint, credential=search_creds)
    search_client = SearchClient(
        endpoint=search_endpoint, credential=search_creds, index_name=searchindex
    )
    form_recognizer_client = DocumentAnalysisClient(
        endpoint=f"https://{formrecognizerservice}.cognitiveservices.azure.com/",
        credential=formrecognizer_creds,
    )
    create_and_populate_index(
        searchindex, index_client, search_client, form_recognizer_client, embeddingkey, embeddingendpoint
    )
    print("Data preparation for index", searchindex, "completed")
