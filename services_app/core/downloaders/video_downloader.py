import os
import requests
from ..constants.video_constants import DEFAULT_TIMEOUT


class VideoDownloader:
    """Handles video downloading from URLs."""
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout

    async def download_video(self, url: str, output_path: str) -> bool:
        """Download video from URL to local path."""
        try:
            response = requests.get(url, timeout=self.timeout, stream=True, verify=False)
            
            if response.status_code != 200:
                raise RuntimeError(f"Download failed: HTTP {response.status_code}")
            
            # Write file synchronously
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
            
            return True
        except requests.exceptions.RequestException as e:
            # Clean up incomplete file if exists
            if os.path.exists(output_path):
                os.remove(output_path)
            raise RuntimeError(f"Download error: {str(e)}")