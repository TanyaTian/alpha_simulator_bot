FROM python:3.9-slim

# 设置正确的阿里云镜像源（关键修改）
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org|mirrors.aliyun.com/debian-security|g' /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security/ bullseye-security main" >> /etc/apt/sources.list && \
    pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir numpy==1.24.3 && \
    pip install --no-cache-dir -r requirements.txt

COPY src/ .
CMD ["python", "main.py"]