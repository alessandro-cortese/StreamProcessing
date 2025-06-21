FROM python:3.10-slim

WORKDIR /app

COPY    requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# CMD ["python3", "src/rest_client.py", "http://micro-challenger:8866"]
