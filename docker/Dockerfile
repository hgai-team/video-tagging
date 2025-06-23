# Sử dụng base image Python chính thức.
FROM python:3.10-slim

USER root

# Thiết lập biến môi trường
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Cài đặt các dependencies hệ thống (ví dụ: ffmpeg cho video_processor.py)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Tạo thư mục làm việc
WORKDIR /workspace

RUN mkdir -p /workspace/model_weight/models--BAAI--bge-m3 \
    && mkdir -p /workspace/logs

# Sao chép file requirements.txt và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]