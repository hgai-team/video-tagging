import asyncio
from ..constants.video_constants import DEFAULT_SUBPROCESS_TIMEOUT, DEFAULT_HD_LIMIT

class VideoProcessor:
    """Handles video format processing and conversion."""
    
    def __init__(self, subprocess_timeout: int = DEFAULT_SUBPROCESS_TIMEOUT):
        self.subprocess_timeout = subprocess_timeout

    async def _run_cmd(self, *args: str) -> tuple[int, str, str]:
        """Run subprocess without blocking event loop, with timeout."""
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
        """Get width, height via ffprobe."""
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
        hd_limit: int = DEFAULT_HD_LIMIT
    ) -> bool:
        """
        Convert video to 1 fps with resolution scaling:
         - Landscape: if width > hd_limit → scale width to hd_limit.
         - Portrait:  if height > hd_limit → scale height to hd_limit.
         - Otherwise keep original dimensions.
        """
        try:
            width, height = await self._get_dimensions(input_path)

            if width >= height:
                if width > hd_limit:
                    scale = f"scale={hd_limit}:-2"
                else:
                    scale = "scale=iw:-2"
            else:
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
                raise RuntimeError(f"ffmpeg failed: {err}")

            return True

        except (FileNotFoundError, asyncio.TimeoutError, Exception):
            return False