# Video Processing Service

FastAPI service for asynchronous video processing and AI-powered content tagging using Google Gemini.

## Features

- **Async Processing**: Handle 100+ concurrent video requests
- **Video Downscaling**: FFmpeg-based video compression to 59 frames
- **AI Tagging**: Gemini 2.0 Flash for comprehensive video content analysis
- **Queue Management**: Built-in task queue with status tracking
- **Error Handling**: 3-retry mechanism with exponential backoff
- **Dual Database**: Separate tracking for results and processing status

## Requirements

### System Dependencies
- Python 3.8+
- FFmpeg (with ffprobe)

## Installation

1. **Clone and setup**:
```bash
git clone https://github.com/hgai-team/video-tagging.git
cd video-tagging
pip install -r requirements.txt
```

2. **Set environment variables**:
```bash
export GEMINI_API_KEY="your_gemini_api_key"
```

3. **Run service**:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

### Submit Video for Processing
```http
POST /process
Content-Type: application/json

{
  "video_url": "https://example.com/video.mp4",
  "file_id": "unique_file_id"
}
```

**Response**:
```json
{
  "file_id": "unique_file_id",
  "status": "queued"
}
```

### Check Processing Status
```http
GET /status/{file_id}
```

**Response**:
```json
{
  "file_id": "unique_file_id",
  "status": "completed",
  "created_at": "2025-05-28T10:00:00",
  "updated_at": "2025-05-28T10:05:00",
  "active_processing": false,
  "result": {
    "tags": {
      "Overview": ["outdoor", "landscape", "mountains"],
      "Entities": {
        "Person": ["group of hikers"],
        "Animal": [],
        "Object": ["backpack", "hiking poles"]
      },
      "ActionsEventsFestivals": ["hiking", "trekking"],
      "SceneContext": {
        "Type": "outdoor",
        "HumanMadeArea": ["trail"],
        "NaturalEnvironment": ["mountain", "forest"]
      },
      "TemporalContext": {
        "PartOfDay": "morning",
        "Season": "summer",
        "Weather": "clear",
        "Lighting": ["natural light", "bright"]
      },
      "AffectiveMoodVibe": ["adventurous", "peaceful", "energetic"],
      "Color": ["green", "blue", "brown"],
      "VisualAttributes": ["natural texture", "scenic view"],
      "CameraTechniques": ["wide shot", "steady cam", "landscape"]
    },
    "processing_time": 15.24,
    "token_usage": {
      "total_tokens": 1247,
      "prompt_tokens": 1150,
      "output_tokens": 97
    }
  }
}
```

### System Statistics
```http
GET /stats
```

**Response**:
```json
{
  "queue_size": 5,
  "active_processing": 12,
  "max_concurrent": 100,
  "available_slots": 88
}
```

### Health Check
```http
GET /health
```

## Processing Status Values

- `queued`: Task submitted and waiting in queue
- `processing`: Video currently being processed
- `completed`: Processing finished successfully
- `failed`: Processing failed after all retries

## Configuration

Edit `config.py` to modify:

```python
MAX_CONCURRENT_TASKS = 100    # Concurrent processing limit
MAX_RETRIES = 3               # Retry attempts for failed tasks
RETRY_DELAY = 10              # Seconds between retries
PROCESSING_TIMEOUT = 120      # Timeout per video (seconds)
DEFAULT_WIDTH = 800           # Video output width
TARGET_FRAMES = 59            # Output video length (frames)
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FastAPI       │    │   Task Manager   │    │ Video Processor │
│   /process      │───▶│   Queue System   │───▶│ Download/FFmpeg │
│   /status       │    │   100 concurrent │    │ Gemini AI       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Status DB     │    │   Results DB     │    │   Temp Files    │
│   (tracking)    │    │   (JSON output)  │    │   (auto-clean)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## File Structure

```
├── main.py              # FastAPI application
├── models.py            # Pydantic data models
├── database.py          # SQLite database operations
├── video_processor.py   # Video processing logic
├── task_manager.py      # Async task management
├── config.py           # Configuration constants
├── requirements.txt    # Dependencies
├── README.md           # This file
├── results.db          # Results database (auto-created)
├── status.db           # Status database (auto-created)
└── video_processor.log # Application logs (auto-created)
```

## Usage Examples

### Process Single Video
```python
import requests

response = requests.post('http://localhost:8000/process', json={
    "video_url": "https://example.com/video.mp4",
    "file_id": "video_001"
})
print(response.json())  # {"file_id": "video_001", "status": "queued"}
```

### Check Status
```python
import requests

response = requests.get('http://localhost:8000/status/video_001')
status = response.json()
print(f"Status: {status['status']}")
```

### Batch Processing
```python
import requests
import asyncio
import aiohttp

async def submit_batch(video_urls):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, url in enumerate(video_urls):
            task = session.post('http://localhost:8000/process', json={
                "video_url": url,
                "file_id": f"batch_{i:03d}"
            })
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        return [await r.json() for r in responses]

# Submit 100 videos
urls = ["https://example.com/video_{i}.mp4" for i in range(100)]
results = asyncio.run(submit_batch(urls))
```

## Troubleshooting

### Common Issues

**FFmpeg not found**:
```bash
# Check installation
ffmpeg -version
ffprobe -version

# Install if missing (Ubuntu)
sudo apt install ffmpeg
```

**Gemini API errors**:
- Verify API key is set: `echo $GEMINI_API_KEY`
- Check quota limits in Google AI Studio
- Ensure API is enabled for your project

**Database permission errors**:
```bash
# Ensure write permissions
chmod 755 .
touch results.db status.db
```

**Memory issues with large videos**:
- Videos are processed in temporary directories and auto-cleaned
- Adjust `PROCESSING_TIMEOUT` for very large files
- Monitor disk space in `/tmp`

### Monitoring

**View logs**:
```bash
tail -f video_processor.log
```

**Check system stats**:
```bash
curl http://localhost:8000/stats
```

**Database inspection**:
```bash
sqlite3 status.db "SELECT file_id, status, updated_at FROM status ORDER BY updated_at DESC LIMIT 10;"
```

## Performance Notes

- Maximum 100 concurrent video processing tasks
- Each video is downscaled to 59 frames (~59 seconds at 1fps)
- Typical processing time: 10-30 seconds per video
- Gemini API rate limits apply (2000 requests/minute)
- Temporary files are automatically cleaned after processing

## License

MIT License
