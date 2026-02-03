FROM python:3.11-slim

WORKDIR /app

# 复制代码和依赖文件
COPY . /app

# 确保 pip 升级并安装依赖
RUN apt-get update && apt-get install -y gcc libffi-dev \
    && python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get remove -y gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
