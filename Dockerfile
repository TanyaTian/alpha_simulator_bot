FROM python:3.9-slim

# 安装系统依赖（pandas需要这些）
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 更新pip并设置国内镜像源
RUN python -m pip install --upgrade pip && \
    pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .
CMD ["python", "main.py"]
