import os
import requests
import msal
import json
from datetime import datetime
import hashlib
import sys
import time
import zipfile
import os

# Configuration
#========
#CLIENT_ID = "YOUR_CLIENT_ID"
#CLIENT_SECRET = "YOUR_CLIENT_SECRET"
#TENANT_ID = "YOUR_TENANT_ID"
#TENANT_ID = "YOUR_TENANT_ID"
Nretry=30
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_URL = "https://graph.microsoft.com/v1.0"

# Local directory to save files
LOCAL_DIR = "./m365_data"
os.makedirs(LOCAL_DIR, exist_ok=True)
versionlog=open(f"{LOCAL_DIR}/{TARGET_USER}-file_version.log","w",encoding="utf-8")
collectionlog=open(f"{LOCAL_DIR}/{TARGET_USER}-collection.log","w",encoding="utf-8")
apilog=open(f"{LOCAL_DIR}/{TARGET_USER}-api.log","w",encoding="utf-8")
#hashfile=open(f"./{TARGET_USER}-onedrive.zip.MD5","w")

current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

sys.stdout.reconfigure(encoding='utf-8')
sys.stdin.reconfigure(encoding='utf-8')
def zip_directory(directory_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                zipf.write(os.path.join(root, file), 
                           os.path.relpath(os.path.join(root, file), 
                                           os.path.join(directory_path, '..')))
def log_print(*args, **kwargs):
    """
    A wrapper function for the print statement that logs messages to both the console and a file,
    including a timestamp for each log entry. Supports all parameters of the original print() function.

    :param args: Variable-length arguments passed to print().
    :param kwargs: Keyword arguments passed to print().
    """
    # Get the current timestamp
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Combine the timestamp with the original message (args)
    log_message = f"[{current_time}] " + " ".join(map(str, args))

    # Print the log message to the console
    print(log_message, **kwargs)
    print(log_message, **kwargs,file=collectionlog)
    sys.stdout.flush()
    collectionlog.flush()

#log api call
def requestsget(url,headers={},stream=False):
   apilog.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - GET "+url+"\n")
   response=requests.get(url,headers=headers,stream=stream)
   for i in range(Nretry):
      if response.status_code!=200: 
          log_print(f"Failed to get: {url}, Status Code: {response.status_code}")
          try:
            log_print(response.text,response.reason)
          except:
            log_print("Failed logging response.text and response.reason")
          log_print(f"Retry getting: {url}")
          time.sleep(1)  
          response=requests.get(url,headers=headers,stream=stream)
          continue
      break
   if response.status_code!=200: raise Exception("api call failed after "+str(Nretry)+" retry")
   return response
# Authenticate and get access token
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception("Failed to acquire token: " + str(result.get("error_description")))

# Download a file from a URL
def download_file(url, access_token, local_path):
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requestsget(url, headers=headers, stream=True)
    savesize=0
    if response.status_code == 200:
        log_print(f"Downloading: {local_path}")
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                savesize+=len(chunk)
                apilog.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - Downloading "+local_path+" at "+str(savesize)+"\n")
                f.write(chunk)

        md5=md5checksum(local_path)
        log_print(f"Downloaded: {local_path} MD5:{md5}")
    else:
        log_print(f"Failed to download: {url} to {local_path}, Status Code: {response.status_code}")

# Recursively retrieve files and folders from OneDrive or SharePoint
def retrieve_folder_contents(access_token, folder_url, local_folder_path, site_id=None):
    headers = {"Authorization": f"Bearer {access_token}"}

    while folder_url:
        response = requestsget(folder_url, headers=headers)
        if response.status_code != 200:
            log_print(f"Error retrieving folder contents: {response.status_code}, {response.text}, {folder_url}")
            break

        data = response.json()
        apilog.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - "+json.dumps(data)+"\n")

        for item in data.get("value", []):
            item_name = item["name"]
            item_id = item["id"]
            item_local_path = os.path.join(local_folder_path, item_name)

            if "folder" in item:
                # Create a local directory for the folder
                os.makedirs(item_local_path, exist_ok=True)
                log_print(f"Created folder: {item_local_path}")

                # Recursively process subfolder
                subfolder_url="Test"
                if site_id is None:
                   subfolder_url = f"{GRAPH_URL}/users/{TARGET_USER}/drive/items/{item_id}/children"
                if site_id:
                   subfolder_url = f"{GRAPH_URL}/sites/{site_id}/drive/items/{item_id}/children"
                versionlog.write(item_local_path+","+json.dumps(item)+"\n")
                retrieve_folder_contents(access_token, subfolder_url, item_local_path)

            elif "file" in item:
                # Save the current version
                file_versions_dir = os.path.join(local_folder_path, f"{item_name}_versions")
                os.makedirs(file_versions_dir, exist_ok=True)

                file_url = item["@microsoft.graph.downloadUrl"]
                current_version_path = os.path.join(file_versions_dir, "current")
                os.makedirs(current_version_path,exist_ok=True)
                current_version_path = os.path.join(file_versions_dir, "current" , f"{item_name}")
                versionlog.write(current_version_path+","+json.dumps(item)+"\n")
                download_file(file_url, access_token, current_version_path)

                # Retrieve and save all versions
                versions_url = f"{GRAPH_URL}/users/{TARGET_USER}/drive/items/{item_id}/versions"
                versions_response = requestsget(versions_url, headers=headers)
                if versions_response.status_code == 200:
                    versions_data = versions_response.json()
                    apilog.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - "+json.dumps(versions_data)+"\n")

                    for version in versions_data.get("value", []):
                        version_id = version["id"]
                        version_name = f"{item_name}" #_version_{version_id}"
                        version_url = f"{GRAPH_URL}/users/{TARGET_USER}/drive/items/{item_id}/versions/{version_id}"
                        version_response = requestsget(version_url, headers=headers)
                        item2 = version_response.json()
                        apilog.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" - "+json.dumps(item2)+"\n")

                        if "@microsoft.graph.downloadUrl" in item2:   
                             version_url2 = item2["@microsoft.graph.downloadUrl"]
                             version_local_path = os.path.join(file_versions_dir, version_id)
                             os.makedirs(version_local_path,exist_ok=True)
                             version_local_path = os.path.join(file_versions_dir, version_id,version_name)
                             versionlog.write(version_local_path+","+json.dumps(item2)+"\n")
                             download_file(version_url2, access_token, version_local_path)
        # Check for next page
        folder_url = data.get("@odata.nextLink")

# Retrieve and save OneDrive files and folders
def retrieve_onedrive_files_and_folders(access_token):
    onedrive_dir = os.path.join(LOCAL_DIR, "OneDrive")
    os.makedirs(onedrive_dir, exist_ok=True)

    # Start with the root folder
    root_folder_url = f"{GRAPH_URL}/users/{TARGET_USER}/drive/root/children"
    retrieve_folder_contents(access_token, root_folder_url, onedrive_dir)

def md5checksum(fname):

    md5 = hashlib.md5()

    # handle content in binary form
    f = open(fname, "rb")

    while chunk := f.read(4096):
        md5.update(chunk)

    return md5.hexdigest()

# Main function
def main():
    log_print("Starting OneDrive data collection at", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    log_print("Collecting OneDrive data of", TARGET_USER)
    try:
        access_token = get_access_token()
        log_print("Access token retrieved successfully.")
    except Exception as e:
        log_print(f"An error occurred: {e}")
        return
    try:
        # Retrieve OneDrive files and folders
        log_print("Retrieving OneDrive files and folders...")
        retrieve_onedrive_files_and_folders(access_token)
    except Exception as e:
        log_print(f"An error occurred: {e}")
        return


    log_print("All data retrieval completed :", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == "__main__":
    main()
    versionlog.close()
    collectionlog.close()
    apilog.close()
    zip_directory(LOCAL_DIR,f"{TARGET_USER}-onedrive.zip")
    open(f"./{TARGET_USER}-onedrive.zip.MD5","w").write(md5checksum(f"{TARGET_USER}-onedrive.zip"))
    print(f"OneDrive data collection completed.")
