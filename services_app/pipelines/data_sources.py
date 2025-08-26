import asyncio
import abc
import logging
import requests
import os
from pathlib import Path
from typing import List, Dict
from config.settings import get_settings
from api.router.stock_router import get_unlabel_resources, get_resources_old_version

logger = logging.getLogger(__name__)

class BaseDataSource(abc.ABC):
    """Abstract class for data sources."""
    @abc.abstractmethod
    async def get_resources(self, **kwargs) -> List[Dict]:
        """Get a list of resources to process. Each resource is a dict with 'id' and 'duration'."""
        pass

class UnlabeledDataSource(BaseDataSource):
    async def get_resources(self, start_date: str, end_date: str, media_type: int, **kwargs) -> List[Dict]:
        logger.info(f"Fetching unlabeled resources from {start_date} to {end_date}")
        return await get_unlabel_resources(
            media_type=media_type,
            start_date=start_date,
            end_date=end_date
        )

class OldVersionDataSource(BaseDataSource):
    async def get_resources(self, current_tag_version: str, media_type: int, **kwargs) -> List[Dict]:
        logger.info(f"Fetching resources with tag version older than {current_tag_version}")
        return await get_resources_old_version(
            current_tag_version=current_tag_version,
            media_type=media_type
        )

class MissUpsertDataSource(BaseDataSource):
    async def get_resources(self, start_date: str, end_date: str, media_type: int, collection_name: str, **kwargs) -> List[Dict]:
        logger.info(f"Fetching miss-upsert resources from {start_date} to {end_date}")
        settings = get_settings()

        # Step 1: Get all resources in time range with their tag versions
        payload_info = {"startDate": start_date, "endDate": end_date, "mediaType": media_type}
        headers_info = {"Accept": "text/plain", "X-Time-Zone": "Asia/Bangkok"}

        try:
            response_info = requests.post(url=settings.FILE_INFO_TAG_VERSION, headers=headers_info, json=payload_info)
            response_info.raise_for_status()
            all_resources = response_info.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get file info by tag version: {e}")
            return []

        # Filter for IDs with the correct tag version
        target_ids = [res['id'] for res in all_resources if res.get('tagVersion') == kwargs.get('tag_version')]

        if not target_ids:
            logger.info("No resources with the target tag version found in the date range.")
            return []

        # Step 2: Check which of these IDs are missing from the vector collection
        headers_retrieve = {"Accept": "application/json", "Content-Type": "application/json"}
        url_retrieve = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/retrieve"

        try:
            # Note: This API seems to return IDs that are *missing*. The original code's logic was a bit unclear.
            # Assuming the API correctly returns the list of IDs that need to be upserted.
            response_retrieve = requests.post(url=url_retrieve, headers=headers_retrieve, json=target_ids, timeout=360)
            response_retrieve.raise_for_status()
            missing_ids = response_retrieve.json()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve missing points from collection: {e}")
            return []

        logger.info(f"Found {len(missing_ids)} missing points to process.")

        # Return in the expected format, using a default duration as it's not provided
        return [{'id': point_id, 'duration': 20} for point_id in missing_ids]

class TaggedUnclassifiedDataSource(BaseDataSource):
    """Data source for videos that are tagged but not classified as real/AI."""
    async def get_resources(self, collection_name: str, **kwargs) -> List[Dict]:
        logger.info(f"Fetching tagged but unclassified videos from collection {collection_name}")
        try:
            settings = get_settings()
            batch_size = kwargs.get('batch_size', 100)
            offset = kwargs.get('offset', 0)
            
            # Use the correct API URL
            url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/tagged"
            
            # Query parameters
            params = {
                "is_real": "untagged",
                "limit": batch_size,
                "offset": offset
            }
            
            headers = {'Accept': 'application/json'}

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: requests.get(url=url, headers=headers, params=params, timeout=60)
            )
            
            # Check response
            if response.status_code != 200:
                logger.error(f"API call failed: {response.status_code} - {response.text}")
                return []
                
            # Parse response
            response_data = response.json()
            if not response_data.get('success', False):
                logger.error(f"API returned error: {response_data.get('error')}")
                return []
                
            video_ids = response_data.get('data', {}).get('video_ids', [])
            
            # Convert to the format expected by the pipeline
            resources = [{'id': video_id, 'duration': 0} for video_id in video_ids]
            
            logger.info(f"Found {len(resources)} tagged but unclassified videos")
            return resources
            
        except Exception as e:
            logger.error(f"Error in TaggedUnclassifiedDataSource: {str(e)}")
            return []

class TxtFileDataSource(BaseDataSource):
    """Data source that reads video IDs from untagged_real_ids.txt file."""
    
    def __init__(self, txt_file_path: str = "untagged_real_ids.txt"):
        self.txt_file_path = Path(txt_file_path)
        
    async def get_resources(self, batch_size: int = 40, **kwargs) -> List[Dict]:
        """Read next batch of video IDs from txt file and remove them."""
        try:
            if not self.txt_file_path.exists():
                logger.info(f"Txt file {self.txt_file_path} does not exist")
                return []
            
            video_ids = self._read_and_remove_batch(batch_size)
            
            if not video_ids:
                logger.info("No more video IDs in txt file")
                return []
            
            resources = [{'id': video_id.strip(), 'duration': 0} for video_id in video_ids if video_id.strip()]
            
            logger.info(f"Loaded {len(resources)} video IDs from txt file")
            return resources
            
        except Exception as e:
            logger.error(f"Error in TxtFileDataSource: {str(e)}")
            return []
    
    def _read_and_remove_batch(self, batch_size: int) -> List[str]:
        """Read next batch of video IDs from txt file and remove them."""
        try:
            if not self.txt_file_path.exists():
                return []
            
            with open(self.txt_file_path, 'r') as f:
                lines = f.readlines()
            
            if not lines:
                return []
            
            # Filter out empty lines first, then take batch
            valid_lines = [line for line in lines if line.strip()]
            if not valid_lines:
                return []
            
            batch_lines = valid_lines[:batch_size]
            
            # Find how many lines to remove from original file (including empty lines)
            batch_count = 0
            lines_to_remove = 0
            for line in lines:
                if batch_count >= batch_size:
                    break
                lines_to_remove += 1
                if line.strip():  # Only count non-empty lines toward batch
                    batch_count += 1
            
            remaining = lines[lines_to_remove:]
            
            with open(self.txt_file_path, 'w') as f:
                f.writelines(remaining)
            
            return [line.strip() for line in batch_lines]
            
        except Exception as e:
            logger.error(f"Error reading batch from txt file: {str(e)}")
            return []

class FileBasedDetectionDataSource(BaseDataSource):
    """File-based data source for detection pipeline that avoids infinite loops."""
    
    def __init__(self, queue_dir: str = "./temp_detection_queues"):
        self.queue_dir = Path(queue_dir)
        self.queue_dir.mkdir(exist_ok=True)
        self.queue_file = None
        
    async def get_resources(self, collection_name: str, batch_size: int = 200, **kwargs) -> List[Dict]:
        try:
            if self.queue_file is None:
                await self._initialize_queue(collection_name)
            
            if not self.queue_file or not self.queue_file.exists():
                logger.info("No queue file exists, no more resources to process")
                return []
            
            video_ids = self._read_next_batch(batch_size)
            
            if not video_ids:
                logger.info("No more video IDs in queue file")
                self._cleanup_queue_file()
                return []
            
            resources = [{'id': video_id.strip(), 'duration': 0} for video_id in video_ids if video_id.strip()]
            
            logger.info(f"Loaded {len(resources)} video IDs from queue file")
            return resources
            
        except Exception as e:
            logger.error(f"Error in FileBasedDetectionDataSource: {str(e)}")
            return []
    
    async def _initialize_queue(self, collection_name: str):
        """Initialize queue file with untagged video IDs, reuse existing if available."""
        try:
            # Check for existing queue files first
            existing_file = self._find_existing_queue_file()
            if existing_file:
                self.queue_file = existing_file
                with open(existing_file, 'r') as f:
                    remaining_count = sum(1 for line in f if line.strip())
                logger.info(f"Found existing queue file: {existing_file} with {remaining_count} IDs remaining")
                return
            
            # No existing file, create new one
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.queue_file = self.queue_dir / f"detection_queue_{timestamp}.txt"
            
            logger.info(f"Creating new detection queue file: {self.queue_file}")
            
            all_video_ids = await self._fetch_all_untagged_videos(collection_name)
            
            if not all_video_ids:
                logger.warning("No untagged videos found to initialize queue")
                return
                
            with open(self.queue_file, 'w') as f:
                for video_id in all_video_ids:
                    f.write(f"{video_id}\n")
            
            logger.info(f"Initialized new queue with {len(all_video_ids)} video IDs")
            
        except Exception as e:
            logger.error(f"Failed to initialize detection queue: {str(e)}")
            self.queue_file = None
    
    def _find_existing_queue_file(self) -> Path:
        """Find the most recent existing queue file that has content."""
        try:
            queue_files = list(self.queue_dir.glob("detection_queue_*.txt"))
            
            # Filter files that have content
            valid_files = []
            for file_path in queue_files:
                try:
                    with open(file_path, 'r') as f:
                        # Check if file has at least one non-empty line
                        for line in f:
                            if line.strip():
                                valid_files.append(file_path)
                                break
                except Exception:
                    continue
            
            if valid_files:
                # Sort by modification time and return the most recent
                valid_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                return valid_files[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding existing queue file: {str(e)}")
            return None
    
    async def _fetch_all_untagged_videos(self, collection_name: str) -> List[str]:
        """Fetch all unique untagged video IDs from the collection."""
        settings = get_settings()
        all_video_ids = set()  # Use set to prevent duplicates
        offset = 0
        batch_size = 1000
        max_fetch_limit = 300000  # Max 300K IDs per queue file
        
        logger.info(f"Starting to fetch untagged videos (max {max_fetch_limit} videos)")
        
        while len(all_video_ids) < max_fetch_limit:
            try:
                url = f"{settings.TAG_DOMAIN}af/collections/{collection_name}/points/tagged"
                params = {
                    "is_real": "untagged",
                    "limit": batch_size,
                    "offset": offset
                }
                headers = {'Accept': 'application/json'}
                
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: requests.get(url=url, headers=headers, params=params, timeout=60)
                )
                
                if response.status_code != 200:
                    logger.error(f"API call failed: {response.status_code} - {response.text}")
                    break
                
                response_data = response.json()
                if not response_data.get('success', False):
                    logger.error(f"API returned error: {response_data.get('error')}")
                    break
                
                video_ids = response_data.get('data', {}).get('video_ids', [])
                
                if not video_ids:
                    logger.info("No more videos found - reached end of collection")
                    break
                
                # Track before adding to detect if we got new unique IDs
                initial_count = len(all_video_ids)
                all_video_ids.update(video_ids)  # Add to set (automatically removes duplicates)
                new_unique_count = len(all_video_ids) - initial_count
                
                logger.info(f"Fetched {len(video_ids)} video IDs (batch), {new_unique_count} new unique IDs, total unique: {len(all_video_ids)}")
                
                # If no new unique IDs were added, we're getting duplicates (end of data)
                if new_unique_count == 0:
                    logger.info("No new unique IDs found - reached end of unique data")
                    break
                
                offset += batch_size
                
                # If batch returned fewer IDs than requested, likely at end
                if len(video_ids) < batch_size:
                    logger.info("Reached end of available videos (batch smaller than limit)")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching untagged videos at offset {offset}: {str(e)}")
                break
        
        final_count = len(all_video_ids)
        if final_count >= max_fetch_limit:
            logger.info(f"Reached fetch limit of {max_fetch_limit} videos")
            # Convert back to list and limit to max_fetch_limit
            return list(all_video_ids)[:max_fetch_limit]
        
        logger.info(f"Fetched total of {final_count} unique untagged video IDs")
        return list(all_video_ids)
    
    def _read_next_batch(self, batch_size: int) -> List[str]:
        """Read next batch of video IDs from queue file and remove them."""
        try:
            if not self.queue_file.exists():
                return []
            
            with open(self.queue_file, 'r') as f:
                lines = f.readlines()
            
            if not lines:
                return []
            
            batch = lines[:batch_size]
            remaining = lines[batch_size:]
            
            with open(self.queue_file, 'w') as f:
                f.writelines(remaining)
            
            return [line.strip() for line in batch]
            
        except Exception as e:
            logger.error(f"Error reading batch from queue file: {str(e)}")
            return []
    
    def _cleanup_queue_file(self):
        """Remove empty queue file."""
        try:
            if self.queue_file and self.queue_file.exists():
                self.queue_file.unlink()
                logger.info(f"Cleaned up empty queue file: {self.queue_file}")
        except Exception as e:
            logger.error(f"Error cleaning up queue file: {str(e)}")
    
    def mark_batch_completed(self, processed_ids: List[str]):
        """Mark a batch as completed (for logging purposes)."""
        logger.info(f"Batch completed: {len(processed_ids)} video IDs processed")