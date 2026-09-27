FROM python:3.12-slim
WORKDIR /bot
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py mongo_store.py ./
CMD ["python", "-u", "bot.py"]
