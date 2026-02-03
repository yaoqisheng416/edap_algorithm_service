# 1️⃣ 使用官方 Python 镜像
FROM python:3.11-slim

# 2️⃣ 设置工作目录
WORKDIR /

# 3️⃣ 拷贝项目文件
COPY . /

# 4️⃣ 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 5️⃣ 设置环境变量（可选）
ENV PYTHONUNBUFFERED=1

# 6️⃣ 启动命令
CMD ["python", "main.py"]
