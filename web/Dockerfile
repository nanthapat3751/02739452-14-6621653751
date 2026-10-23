FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["gunicorn", "-b", "0.0.0.0:8501", "-w", "2", "app:app"]
