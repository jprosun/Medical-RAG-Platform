import requests

QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "staging_medqa"

print(f"Deleting collection {COLLECTION_NAME}...")
res = requests.delete(f"{QDRANT_URL}/collections/{COLLECTION_NAME}")
if res.status_code == 200:
    print("Collection deleted successfully!")
else:
    print(f"Failed to delete (might not exist): {res.status_code} - {res.text}")
