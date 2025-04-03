import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import json

# Hardcoded connection strings (Replace with actual values)
SOURCE_CONNECTION_STRING = ""
DEST_CONNECTION_STRING = ""

def transfer_file(source_container, source_blob, dest_container, dest_blob):
    """ Transfers a file between Azure Storage accounts """
    try:
        # Connect to source storage account
        source_blob_service = BlobServiceClient.from_connection_string(SOURCE_CONNECTION_STRING)
        source_blob_client = source_blob_service.get_blob_client(container=source_container, blob=source_blob)




        # Download file into memory
        file_data = source_blob_client.download_blob().readall()

        # Connect to destination storage account
        dest_blob_service = BlobServiceClient.from_connection_string(DEST_CONNECTION_STRING)
        dest_blob_client = dest_blob_service.get_blob_client(container=dest_container, blob=dest_blob)

        # Upload the file to the destination storage
        dest_blob_client.upload_blob(file_data, overwrite=True)

        return f"File '{source_blob}' successfully transferred to '{dest_container}'"

    except Exception as e:
        return f"Error: {str(e)}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    """ Azure Function entry point for HTTP-triggered file transfer """
    try:
        req_body = req.get_json()
        source_container = req_body.get("source_container")
        source_blob = req_body.get("source_blob")
        dest_container = req_body.get("dest_container")
        dest_blob = req_body.get("dest_blob", source_blob)  # Default: same name

        if not source_container or not source_blob or not dest_container:
            return func.HttpResponse("Missing required parameters.", status_code=400)

        result = transfer_file(source_container, source_blob, dest_container, dest_blob)
        return func.HttpResponse(json.dumps({"message": result}), mimetype="application/json", status_code=200)

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
        
