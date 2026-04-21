from python:3.11-slim

workdir /app

copy requirements.txt /app/requirements.txt
run pip install --no-cache-dir -r /app/requirements.txt

copy . /app

entrypoint ["python", "main.py"]

