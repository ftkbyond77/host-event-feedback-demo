NGROK_TOKEN = 3CcHMKGRn7kVcefd2AbpILfOiRy_6J3qzamWXxQ7TCoQTgqVj

=====
Step 1:
```
docker build -t braze-mock-server .
```

Step 2:
```
docker run -p 8000:8000 -p 4040:4040 -e NGROK_AUTHTOKEN=3CcHMKGRn7kVcefd2AbpILfOiRy_6J3qzamWXxQ7TCoQTgqVj -v "$(pwd):/app" braze-mock-server 
```

Step 3 (Stop):
```
docker stop ____ (from CTN docker ps)
```


Pull Pub/Sub (Watch Result)
```
gcloud pubsub subscriptions pull feedback-event-msg-sub \
  --project=ingestion-streaming \
  --limit=10 \
  --auto-ack
```


Demo reset

curl -X POST http://localhost:8000/api/reset
# → reopened=14 — card ทั้งหมดกลับมาบน browser
# → done_flag=FALSE ทุก campaign
# → window ต่ออีก 7 วัน