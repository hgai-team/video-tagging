import logging
from pathlib import Path
from typing import List, Dict, Tuple
import asyncio
import json

from api.router.stock_router import send_tags
from api.router.audio_router import process_audio, audio_tagging, delete_audio, analyze_audio, chords_progression
from api.router.tags_router import upsert_points
from core import audio_pro
from pipelines.base_pipeline import BasePipelineProcessor
from utils.db import task_tracker
from utils.error_logger import log_download_error

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
            error_msg = f"No download URL found in url_data"
            log_download_error(resource_id, "N/A", error_msg)
            task_tracker.mark_failed(resource_id, batch_id, error_msg)
            logger.error(f"{error_msg} for audio {resource_id}. Available fields: {list(url_data.keys() if isinstance(url_data, dict) else [])}")
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
            success, error_msg = await audio_pro.download_audio(url=download_url, output_path=str(output_path))
            
            if not success:
                # Log any download error to the new CSV
                log_download_error(resource_id, download_url, error_msg)
                
                task_tracker.mark_failed(resource_id, batch_id, error_msg)
                logger.error(f"Download audio failed - resource_id={resource_id}, error={error_msg}")
                raise RuntimeError(f"Audio download failed: {error_msg}")

            if not output_path.exists():
                error_msg = f"Downloaded audio file not found at path: {output_path}"
                log_download_error(resource_id, download_url, error_msg)
                task_tracker.mark_failed(resource_id, batch_id, error_msg)
                raise FileNotFoundError(error_msg)

            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Downloaded audio file size for {resource_id}: {file_size_mb:.2f}MB")
            
            if file_size_mb < self.config.min_media_size_mb:
                error_msg = f"Audio file size too small: {file_size_mb:.2f}MB < {self.config.min_media_size_mb}MB"
                log_download_error(resource_id, download_url, error_msg)
                logger.warning(f"Audio file too small ({file_size_mb:.2f}MB), skipping - resource_id={resource_id}")
                output_path.unlink()  
                task_tracker.mark_failed(resource_id, batch_id, error_msg)
                raise ValueError(error_msg)

            logger.info(f"Audio download completed - resource_id={resource_id}, size={file_size_mb:.2f}MB, duration={duration}s")
            return {
                'resource_id': resource_id, 
                'duration': duration,
                'original_path': str(output_path)
            }

        except Exception as e:
            # This block catches the raised exceptions and ensures cleanup
            if output_path.exists():
                try:
                    output_path.unlink()
                except Exception as del_err:
                    logger.error(f"Failed to delete partial audio file {output_path}: {del_err}")
            
            # The error has already been logged and tracked, so we just re-raise
            # to stop processing for this specific resource.
            raise
    
    def _extract_download_url_from_result(self, result: Dict) -> str:
        """Extract download URL from API result for audio."""
        if not result or not isinstance(result, dict):
            return None
            
        url_fields = ['url', 'download_url', 'downloadUrl', 'link', 'videoUrl', 'audioUrl']
        
        url = next((result.get('data', {}).get(f) for f in url_fields if f in result.get('data', {})), None)
        if not url:
            url = next((result.get(f) for f in url_fields if f in result), None)
            
        return url

    async def _run_creative_analysis(self, processed_path: str, resource_id: str) -> Dict:
        """Runs AI tagging on the processed audio file."""
        logger.info(f"Starting creative analysis for resource_id={resource_id}")
        tagging_result = await audio_tagging(input_path=processed_path)
        if not tagging_result or not tagging_result.get('success', False):
            raise RuntimeError(f"Audio tagging failed - resource_id={resource_id}")
        
        tags_data = tagging_result.get('data', {})
        if "tempo" in tags_data and isinstance(tags_data["tempo"], str):
            tags_data["tempo"] = [tags_data["tempo"]]
            
        logger.info(f"Creative analysis successful for resource_id={resource_id}")
        return tags_data

    async def _run_music_theory_analysis(self, original_path: str, resource_id: str) -> Dict:
        """Runs BPM/Key and Chords analysis on the original audio file in parallel."""
        logger.info(f"Starting music theory analysis for resource_id={resource_id}")
        
        metrics_task = asyncio.create_task(analyze_audio(input_path=original_path))
        chords_task = asyncio.create_task(chords_progression(input_path=original_path))
        
        results = await asyncio.gather(metrics_task, chords_task, return_exceptions=True)
        
        # Metrics analysis is critical
        metrics_result = results[0]
        if isinstance(metrics_result, Exception) or not metrics_result.get('success', False):
            raise RuntimeError(f"Audio metrics analysis failed - resource_id={resource_id}: {metrics_result}")
        
        # Chord analysis is optional, default to empty if failed
        chords_result = results[1]
        if isinstance(chords_result, Exception) or not chords_result.get('success', False):
            logger.warning(f"Chord progression analysis failed for resource_id={resource_id}, proceeding with empty chords: {chords_result}")
            chords_data = {"intro_chords": [], "outro_chords": []}
        else:
            chords_data = chords_result.get('data', {"intro_chords": [], "outro_chords": []})

        # Combine results
        theory_data = {
            "bpm": int(metrics_result.get('data', {}).get("bpm", 0)),
            "tone": str(metrics_result.get('data', {}).get("tone", "")),
            "scale": str(metrics_result.get('data', {}).get("scale", "")),
            "intro_chords": list(chords_data.get("intro_chords", [])),
            "outro_chords": list(chords_data.get("outro_chords", [])),
        }
        
        logger.info(f"Music theory analysis successful for resource_id={resource_id}")
        return theory_data

    async def _process_media(self, file_info: Dict, batch_id: str):
        resource_id = file_info['resource_id']
        original_path = file_info['original_path']
        duration = file_info['duration']
        processed_path = str(self.config.temp_dir / f"{resource_id}_processed.wav")
        cleanup_files = [('original', original_path), ('processed', processed_path)]

        try:
            logger.info(f"Starting integrated audio processing for resource_id={resource_id}")

            # Step 1: Process audio for AI tagging (this is a prerequisite for creative analysis)
            process_result = await process_audio(input_path=original_path, output_path=processed_path)
            if not process_result or not process_result.get('success', False):
                raise RuntimeError(f"Audio processing (for tagging) failed - resource_id={resource_id}")
            logger.info(f"Step 1 (Preprocessing) complete - resource_id={resource_id}")

            # Step 2: Start creative analysis and music theory analysis concurrently
            logger.info(f"Step 2: Starting creative analysis - resource_id={resource_id}")
            creative_task = asyncio.create_task(self._run_creative_analysis(processed_path, resource_id))
            theory_task = asyncio.create_task(self._run_music_theory_analysis(original_path, resource_id))
            
            # Wait for creative analysis to complete
            creative_data = await creative_task
            logger.info(f"Step 2A (Creative Analysis) complete - resource_id={resource_id}")

            # Step 3: Send tags immediately after creative analysis
            logger.info(f"Step 3: Sending creative tags - resource_id={resource_id}")
            await send_tags(id=resource_id, tags=creative_data, tag_version=self.config.tag_version)
            logger.info(f"Step 3 (Send Creative Tags) complete - resource_id={resource_id}")

            # Step 4: Wait for music theory analysis to complete
            theory_data = await theory_task
            logger.info(f"Step 4 (Music Theory Analysis) complete - resource_id={resource_id}")

            # Step 5: Combine all data into a final payload
            final_payload = {}
            final_payload.update(creative_data)
            final_payload.update(theory_data)
            final_payload["time"] = duration
            logger.info(f"Step 5 (Combine Data) complete - resource_id={resource_id}: {json.dumps(final_payload)}")

            # Step 6: Upsert vectors (send_tags already completed)
            logger.info(f"Step 6: Starting vector upsert tasks - resource_id={resource_id}")
            tasks = [
                upsert_points(collection_name=self.config.collection_name, points=[final_payload], ids=[resource_id], media_type="audio"),
                self._upsert_stock_vector(final_payload, resource_id, media_type="audio")
            ]
            upsert_results = await asyncio.gather(*tasks, return_exceptions=True)

            failed_tasks = [res for res in upsert_results if isinstance(res, Exception)]
            if failed_tasks:
                raise RuntimeError(f"One or more vector upsert steps failed for audio: {failed_tasks}")

            logger.info(f"Step 6 (Vector Upsert) complete - resource_id={resource_id}")
            task_tracker.mark_success(resource_id, batch_id)
            logger.info(f"Successfully processed audio resource_id={resource_id}")

        except Exception as e:
            task_tracker.mark_failed(resource_id, batch_id, str(e))
            logger.error(f"Audio pipeline failed for resource_id={resource_id}, error={e}", exc_info=True)
        finally:
            # Step 7: Always clean up files
            await self._cleanup_media_files(cleanup_files, resource_id)

    async def _cleanup_media_files(self, cleanup_files: List[Tuple[str, str]], resource_id: str, batch_id: str = None):
        logger.info(f"Step 7: Cleaning up audio files for resource_id={resource_id}")
        for file_type, file_path_str in cleanup_files:
            if file_path_str:
                file_path = Path(file_path_str)
                if file_path.exists():
                    try:
                        await delete_audio(input_path=str(file_path))
                        logger.info(f"Deleted {file_type} audio file successfully: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_type} audio file {file_path}: {e}")