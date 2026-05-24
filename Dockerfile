FROM ngrok/ngrok:latest AS ngrok_base

FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y jq procps && rm -rf /var/lib/apt/lists/*

COPY --from=ngrok_base /bin/ngrok /usr/local/bin/ngrok

RUN mkdir -p /app/data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh

# (8000: FastAPI, 4040: Ngrok Dashboard)
EXPOSE 8000 4040

CMD ["./start.sh"]