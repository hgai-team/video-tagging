import os
import requests
from ..constants.video_constants import DEFAULT_TIMEOUT


class AudioDownloader:
    """Handles audio downloading from URLs."""
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        self.timeout = timeout

    async def download_audio(self, url: str, output_path: str) -> tuple[bool, str]:
        """Download audio from URL to local path."""
        try:
            response = requests.get(url, timeout=self.timeout, stream=True, verify=False)
            
            if response.status_code != 200:
                if response.status_code == 404:
                    return False, f"HTTP 404"
                return False, f"HTTP {response.status_code}"
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  
                        f.write(chunk)
            
            return True, "Success"
        
        except requests.exceptions.RequestException as e:
            if os.path.exists(output_path):
                os.remove(output_path)
            return False, f"Request error: {str(e)}"