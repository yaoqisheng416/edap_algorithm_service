FROM python:3.11-slim

WORKDIR /app

# 复制项目文件
COPY . /app

# 安装系统依赖和 Python 包
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libffi-dev libssl-dev libsnappy-dev \
    && python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get remove -y gcc g++ \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
