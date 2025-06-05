import requests

from typing import Dict, Any

from fastapi import APIRouter, HTTPException

from settings import get_settings

app = APIRouter(
    prefix='/stock',
    tags=['Stocks']
)

@app.get(
    '/resources/unlabel',
)
async def get_unlabel_resources(
    *,
    media_type: int,
    start_date: str,
    end_date: str
):
    payload = {
        "mediaType": media_type,
        "startDate": start_date,
        "endDate": end_date,
    }

    headers = {
        "Accept": "text/plain",
        "X-Time-Zone": "Asia/Bangkok",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            url=get_settings().UNLABEL_URL,
            headers=headers,
            json=payload
        )

        if response.status_code == 200:
            return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    '/resources/old-version',
)
async def get_resources_old_version(
    *,
    current_tag_version: str,
    media_type: int,
):
    params = {
        "currentTagVersion": current_tag_version,
        "mediaType": media_type,
    }

    headers = {
        "Accept": "text/plain",
        "X-Time-Zone": "Asia/Bangkok",
    }

    try:
        response = requests.get(
            url=get_settings().OLD_VERSION_URL,
            headers=headers,
            params=params
        )

        if response.status_code == 200:
            return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    '/resources/download',
)
async def get_download_url(
    *,
    id: str,
):

    headers = {
        "Accept": "text/plain",
        "X-Time-Zone": "Asia/Bangkok",
    }

    try:
        response = requests.get(
            url=get_settings().DOWNLOAD_URL.format(id=id),
            headers=headers,
        )

        if response.status_code == 200:
            return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post(
    '/resources/tags',
)
async def send_tags(
    *,
    id: str,
    tags: Dict[str, Any],
    tag_version: str
):
    def flatten_to_list_dict(d: dict) -> dict[str, list]:
        flat: dict[str, list] = {}
        for key, value in d.items():
            if isinstance(value, dict):
                for sub_key, sub_val in value.items():
                    new_key = f"{key}_{sub_key}"
                    if isinstance(sub_val, list):
                        # Loại bỏ các giá trị trùng lặp trong list
                        flat[new_key] = list(dict.fromkeys(sub_val))
                    else:
                        flat[new_key] = [sub_val]
            else:
                if isinstance(value, list):
                    # Loại bỏ các giá trị trùng lặp trong list
                    flat[key] = list(dict.fromkeys(value))
                else:
                    flat[key] = [value]
        return flat

    tags = flatten_to_list_dict(tags)
    if 'video_description' in tags:
        del tags['video_description']

    headers = {
        "Accept": "text/plain",
        "X-Time-Zone": "Asia/Bangkok",
    }

    payload = {
        "tags": tags,
        "tagVersion": tag_version,
    }

    try:
        response = requests.post(
            url=get_settings().TAGS_URL.format(id=id),
            headers=headers,
            json=payload
        )

        if response.status_code == 200:
            return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
