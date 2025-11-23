S3 to GCS Replication Service
Copy files from AWS S3 to Google Cloud Storage automatically! 🚀

Quick Start
bash# 1. Install
pip install -r requirements.txt

# 2. Configure .env file
cp .env.example .env
# Add your AWS & GCP credentials

# 3. Run
python app.py

# Service runs on http://localhost:5000

Setup Credentials
AWS (IAM User)

AWS Console → IAM → Create user: replication-service
Permissions: AmazonS3ReadOnlyAccess
Create access key → Download CSV
Add to .env:

bashAWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=wJal...
AWS_REGION=ap-south-1
GCP (Service Account)

GCP Console → Service Accounts → Create: replication-service
Role: Storage Object Admin
Create JSON key → Download
Bucket → Permissions → Add service account
Add to .env:

bashGOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
GCS_BUCKET_NAME=your-bucket-name

Test
Health Check:
bashcurl http://localhost:5000/health
Replicate File:
bashcurl -X POST http://localhost:5000/v1/replicate \
  -H "Content-Type: application/json" \
  -d '{"s3_bucket":"my-bucket","s3_key":"file.csv"}'
Response:
json{
  "status": "success",
  "s3_path": "s3://my-bucket/file.csv",
  "gcs_path": "gs://gcs-bucket/my-bucket/file.csv"
}

How It Works
1. Request → Get S3 file metadata
2. Check if already exists in GCS (idempotency)
3. Stream download from S3 (8MB chunks)
4. Stream upload to GCS
5. Save metadata → Return success


Troubleshooting
ProblemSolutioncredentials not foundCheck .env file paths403 permission deniedAdd service account to GCS bucketS3 object not foundVerify bucket name & file pathEndpoint not foundUse /v1/replicate not /replicate

File Structure
.
├── app.py              # Main service
├── requirements.txt    # Dependencies
├── .env               # Your credentials (don't commit!)
├── .env.example       # Template
└── README.md          # This file

API Endpoints
GET /health - Health check
POST /v1/replicate - Copy file from S3 to GCS
Request Body:
json{
  "s3_bucket": "source-bucket",
  "s3_key": "path/to/file.csv"
}

Configuration (.env)
bash# Required
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=wJal...
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
GCS_BUCKET_NAME=your-bucket

