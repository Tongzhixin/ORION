import httpx
import json

class AlluxioPathStatus:
    def __init__(self, mType, mName, mPath, mUfsPath, mLastModificationTimeMs, mHumanReadableFileSize, mLength):
        self.mType = mType
        self.mName = mName
        self.mPath = mPath
        self.mUfsPath = mUfsPath
        self.mLastModificationTimeMs = mLastModificationTimeMs
        self.mHumanReadableFileSize = mHumanReadableFileSize
        self.mLength = mLength

def listdir(path):
    try:
        
        url = f"http://localhost:29999/api/v1/paths/{path}/get-status"
        
        headers = {
            "Content-Type": "application/json"
        }

        with httpx.Client(http2=True,verify=False) as client:
            params = {"path": path}
            response = client.post(url, headers=headers)
            response.raise_for_status()

            result = []
            for data in json.loads(response.content):
                result.append(
                    AlluxioPathStatus(
                        data["mType"],
                        data["mName"],
                        data["mPath"],
                        data["mUfsPath"],
                        data["mLastModificationTimeMs"],
                        data["mHumanReadableFileSize"],
                        data["mLength"],
                    )
                )
            return result
    except httpx.HTTPStatusError as e:
        raise Exception(
            f"Error when listing path {path}: HTTP error {e.response.status_code}"
        ) from e
    except httpx.RequestError as e:
        raise Exception(
            f"Error when listing path {path}: Request error {e}"
        ) from e
    except json.JSONDecodeError as e:
        raise Exception(
            f"Error when listing path {path}: JSON decode error {e}"
        ) from e
    except Exception as e:
        raise Exception(
            f"Error when listing path {path}: error {e}"
        ) from e

# 调用示例
try:

    contents = listdir("/tpch_100g_csv_small/lineitem")
    print(contents)
except Exception as e:
    print(e)