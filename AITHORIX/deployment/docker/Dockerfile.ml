FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-m", "models.ensemble"]
