FROM python:3.9-slim

# 设置阿里云镜像源（APT和Pip）
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org|mirrors.aliyun.com/debian-security|g' /etc/apt/sources.list && \
    pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 安装系统依赖（最小化安装）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 先单独安装numpy（pandas的依赖）
COPY requirements.txt .
RUN pip install --no-cache-dir numpy==1.24.3 && \
    pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY src/ .

# 设置容器启动命令
CMD ["python", "main.py"]