FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirement.txt

EXPOSE 8003

CMD ["python", "main.py"] 