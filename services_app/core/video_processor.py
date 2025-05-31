import os
import asyncio
import logging

import aiohttp
import aiofiles

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/video_processor.log"), logging.StreamHandler()]
)


class VideoProcessor:
    def __init__(self, timeout: int = 360, subprocess_timeout: int = 360):
        self.timeout = timeout
        self.subprocess_timeout = subprocess_timeout
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session is not None and not self.session.closed:
            await self.session.close()
        return False

    async def download_video(self, url: str, output_path: str) -> bool:
        """Download video from URL to local path (async, non-blocking)."""
        if not self.session:
            raise RuntimeError("ClientSession not initialized; use 'async with VideoProcessor()'.")
        async with self.session.get(url) as resp:
            if resp.status != 200:
                logger.error(f"Download failed: HTTP {resp.status}")
                raise RuntimeError(f"Download failed: HTTP {resp.status}")
            async with aiofiles.open(output_path, 'wb') as f:
                async for chunk in resp.content.iter_chunked(8192):
                    await f.write(chunk)
        return True

    async def _run_cmd(self, *args: str) -> tuple[int, str, str]:
        """Chạy subprocess không block event loop, với timeout."""
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=self.subprocess_timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        return proc.returncode, out.decode().strip(), err.decode().strip()

    async def _get_dimensions(self, input_path: str) -> tuple[int, int]:
        """Lấy width, height qua ffprobe."""
        if not os.path.isfile(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'csv=p=0:s=x',
            input_path
        ]
        rc, out, err = await self._run_cmd(*cmd)
        if rc != 0 or 'x' not in out:
            raise RuntimeError(f"ffprobe error: {err or out}")
        w_str, h_str = out.split('x', 1)
        return int(w_str), int(h_str)

    async def shrink_video(
        self,
        input_path: str,
        output_path: str,
        hd_limit: int = 720
    ) -> bool:
        """
        Convert any video to 1 fps, non-blocking:
         - Landscape: if width > hd_limit → scale width to hd_limit.
         - Portrait:  if height > hd_limit → scale height to hd_limit.
         - Otherwise giữ nguyên kích thước.
        """
        try:
            width, height = await self._get_dimensions(input_path)

            # Chọn filter scale
            if width >= height:
                # landscape
                if width > hd_limit:
                    scale = f"scale={hd_limit}:-2"
                else:
                    scale = "scale=iw:-2"
            else:
                # portrait
                if height > hd_limit:
                    scale = f"scale=-2:{hd_limit}"
                else:
                    scale = "scale=-2:ih"

            vf = f"fps=1,{scale}"

            cmd = [
                'ffmpeg', '-y',
                '-i', input_path,
                '-vf', vf,
                '-an',
                output_path
            ]
            rc, out, err = await self._run_cmd(*cmd)
            if rc != 0:
                logger.error(f"ffmpeg failed: {err}")
                raise RuntimeError(f"ffmpeg failed: {err}")

            return True

        except FileNotFoundError as fnf:
            logger.error(fnf)
            return False
        except asyncio.TimeoutError:
            logger.error("Processing timeout (ffprobe/ffmpeg took too long)")
            return False
        except Exception as e:
            logger.error(f"Video processing error: {e}")
            return False
