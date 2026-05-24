# ---------------------------------------------------
# Stage 1: ไปยืมตัว Ngrok มาจาก Official Docker Hub
# ---------------------------------------------------
FROM ngrok/ngrok:latest AS ngrok_base

# ---------------------------------------------------
# Stage 2: สร้าง Server ของเรา
# ---------------------------------------------------
FROM python:3.9-slim

WORKDIR /app

# ติดตั้งแค่เครื่องมือที่จำเป็น (เอา curl ออกไปเลย เพราะไม่ต้องใช้โหลดแล้ว)
RUN apt-get update && apt-get install -y jq procps && rm -rf /var/lib/apt/lists/*

# คัดลอกโปรแกรม ngrok จาก Stage 1 มาไว้ใน Server ของเรา
COPY --from=ngrok_base /bin/ngrok /usr/local/bin/ngrok

# สร้างพื้นที่เก็บ Database Persistent
RUN mkdir -p /app/data

# ติดตั้ง Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอกโค้ดโปรเจกต์ทั้งหมด
COPY . .

# ให้สิทธิ์รันไฟล์ Script
RUN chmod +x start.sh

# เปิด Port (8000: FastAPI, 4040: Ngrok Dashboard)
EXPOSE 8000 4040

# เริ่มทำงาน
CMD ["./start.sh"]