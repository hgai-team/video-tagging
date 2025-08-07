import logging
from pathlib import Path
from typing import List, Dict, Tuple
import asyncio
from api.router.stock_router import send_tags
from api.router.audio_router import process_audio, audio_tagging, delete_audio
from api.router.tags_router import upsert_points
from core import audio_pro
from pipelines.base_pipeline import BasePipelineProcessor
from utils.db import task_tracker

logger = logging.getLogger(__name__)

class AudioPipelineProcessor(BasePipelineProcessor):

    def _count_temp_files(self) -> int:
        return len([f for f in self.config.temp_dir.iterdir() 
                  if f.is_file() and f.suffix in ['.mp3', '.wav']])
    
    def _get_file_patterns(self) -> List[str]:
        return ["*.mp3", "*.wav"]
        
    def _get_temp_files_list(self) -> List[Tuple[str, str]]:
        temp_files = []
        for file_path in self.config.temp_dir.glob("*.mp3"):
            if file_path.is_file():
                temp_files.append(('temp', str(file_path)))
        for file_path in self.config.temp_dir.glob("*.wav"):
            if file_path.is_file():
                temp_files.append(('temp', str(file_path)))
        return temp_files
    
    async def _download_media_safe(self, resource_id: str, url_data: Dict, duration: int, batch_id: str) -> Dict:
        task_tracker.start_task(resource_id, batch_id)

        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl', 'audioUrl']
        download_url = next((url_data.get('data', {}).get(f) for f in url_fields if f in url_data.get('data', {})), None)
        
        if not download_url:
            download_url = next((url_data.get(f) for f in url_fields if f in url_data), None)
        
        if not download_url:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"No download URL found for audio {resource_id}. Available fields: {list(url_data.keys() if isinstance(url_data, dict) else [])}")
            if 'data' in url_data and isinstance(url_data['data'], dict):
                logger.error(f"Data fields: {list(url_data['data'].keys())}")
            raise ValueError(f"No download URL found for audio {resource_id}")

        logger.info(f"Found download URL for audio {resource_id}: {download_url[:50]}...")
        
        file_ext = '.mp3'  
        if download_url.lower().endswith('.wav'):
            file_ext = '.wav'
            
        output_path = self.config.temp_dir / f"{resource_id}{file_ext}"

        try:
            logger.info(f"Starting download for audio {resource_id} to {output_path}")
            result = await audio_pro.download_audio(url=download_url, output_path=str(output_path))
            
            if not result:
                raise RuntimeError(f"Audio download failed with AudioProcessor")

            if not output_path.exists():
                raise FileNotFoundError(f"Downloaded audio file not found: {output_path}")

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded audio file size for {resource_id}: {file_size_mb:.2f}MB")
            
            if file_size_mb < self.config.min_media_size_mb:  # Sử dụng giá trị từ config
                logger.warning(f"Audio file too small ({file_size_mb:.2f}MB), skipping - resource_id={resource_id}")
                output_path.unlink()  
                task_tracker.mark_failed(resource_id, batch_id)  # Đánh dấu là lỗi
                raise ValueError(f"Audio file size too small: {file_size_mb:.2f}MB < {self.config.min_video_size_mb}MB")

            logger.info(f"Audio download completed - resource_id={resource_id}, size={file_size_mb:.2f}MB, duration={duration}s")
            return {
                'resource_id': resource_id, 
                'duration': duration,
                'original_path': str(output_path)
            }

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Download audio failed - resource_id={resource_id}, error={e}", exc_info=True)
            if output_path.exists():
                try:
                    output_path.unlink()
                except Exception as del_err:
                    logger.error(f"Failed to delete partial audio file {output_path}: {del_err}")
            raise

    async def _process_media(self, file_info: Dict, batch_id: str):
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        duration = file_info['duration']
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.wav")
        cleanup_files = [('original', original_path), ('processed', processed_path)]

        try:
            logger.info(f"Bắt đầu luồng xử lý audio cho resource_id={resource_id}")

            process_result = await process_audio(input_path=original_path, output_path=processed_path)
            if not process_result or not process_result.get('success', False):
                raise RuntimeError(f"Audio processing failed - resource_id={resource_id}")
            logger.info(f"Step 4 (Processing) hoàn thành - resource_id={resource_id}")

            tagging_result = await audio_tagging(input_path=processed_path)
            if not tagging_result or not tagging_result.get('success', False):
                raise RuntimeError(f"Audio tagging failed - resource_id={resource_id}")
            tags_data = tagging_result.get('data', {})
        
            if "tempo" in tags_data and isinstance(tags_data["tempo"], str):
                tags_data["tempo"] = [tags_data["tempo"]]               
            tags_data["time"] = duration
                
            logger.info(f"Step 5 (Tagging) hoàn thành - resource_id={resource_id}")

            tasks = []
            tasks.append(send_tags(id=resource_id, tags=tags_data, tag_version=self.config.tag_version))
            tasks.append(upsert_points(collection_name=self.config.collection_name, points=[tags_data], ids=[resource_id], media_type="audio"))
            tasks.append(self._upsert_stock_vector(tags_data, resource_id, media_type="audio"))
            results = await asyncio.gather(*tasks, return_exceptions=True)

            failed_tasks = [res for res in results if isinstance(res, Exception)]
            if failed_tasks:
                raise RuntimeError(f"One or more post-processing steps failed for audio: {failed_tasks}")

            logger.info(f"Steps 6&7 (Upsert/Send) hoàn thành - resource_id={resource_id}")
            task_tracker.mark_success(resource_id, batch_id)
            logger.info(f"Xử lý thành công audio resource_id={resource_id}")

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id)
            logger.error(f"Audio pipeline xử lý thất bại - resource_id={resource_id}, error={e}", exc_info=True)
        finally:
            # Step 8: Luôn luôn dọn dẹp file
            await self._cleanup_media_files(cleanup_files, resource_id)

    async def _cleanup_media_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str = None):
        logger.info(f"Step 8: Cleaning up audio files for resource_id={resource_id}")
        for file_type, file_path_str in cleanup_files:
            if file_path_str:
                file_path = Path(file_path_str)
                if file_path.exists():
                    try:
                        await delete_audio(input_path=str(file_path))
                        logger.info(f"Deleted {file_type} audio file successfully: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_type} audio file {file_path}: {e}")