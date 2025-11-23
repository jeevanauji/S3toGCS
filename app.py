"""
S3 to GCS Replication Service
Event-driven worker for multi-cloud file replication
"""

from flask import Flask, request, jsonify
import boto3
from google.cloud import storage
from botocore.exceptions import ClientError, BotoCoreError
from google.api_core import exceptions as gcs_exceptions
import os
import logging
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '8388608'))


class ReplicationService:
    
    def __init__(self):
        self.s3_client = None
        self.gcs_client = None
        self.gcs_bucket = None
        self._initialize_clients()
    
    def _initialize_clients(self):
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=AWS_REGION,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            logger.info("AWS S3 client initialized successfully")
            
            self.gcs_client = storage.Client()
            
            if not GCS_BUCKET_NAME:
                raise ValueError("GCS_BUCKET_NAME environment variable not set")
            
            self.gcs_bucket = self.gcs_client.bucket(GCS_BUCKET_NAME)
            logger.info(f"GCS client initialized for bucket: {GCS_BUCKET_NAME}")
            
        except Exception as e:
            logger.error(f"Failed to initialize clients: {str(e)}")
            raise
    
    def _get_s3_metadata(self, bucket, key):
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'etag': response.get('ETag', '').strip('"'),
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified')
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}")
            raise
    
    def _check_gcs_exists(self, gcs_key, s3_etag):
        try:
            blob = self.gcs_bucket.blob(gcs_key)
            
            if blob.exists():
                blob.reload()
                stored_etag = blob.metadata.get('s3_etag') if blob.metadata else None
                
                if stored_etag == s3_etag:
                    logger.info(f"File already exists with same ETag: {gcs_key}")
                    return True
                else:
                    logger.info(f"File exists but ETag mismatch, will re-upload: {gcs_key}")
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking GCS existence: {str(e)}")
            return False
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except (ClientError, BotoCoreError, gcs_exceptions.GoogleAPIError) as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                
                wait_time = 2 ** attempt
                logger.warning(
                    f"Attempt {attempt + 1} failed: {str(e)}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
    
    def _stream_upload(self, s3_bucket, s3_key, gcs_key, s3_metadata):
        try:
            s3_response = self.s3_client.get_object(
                Bucket=s3_bucket,
                Key=s3_key
            )
            
            blob = self.gcs_bucket.blob(gcs_key)
            
            blob.metadata = {
                's3_etag': s3_metadata['etag'],
                's3_bucket': s3_bucket,
                's3_key': s3_key,
                'replicated_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            logger.info(f"Starting streaming upload: {s3_key} -> {gcs_key}")
            
            with s3_response['Body'] as s3_stream:
                blob.upload_from_file(
                    s3_stream,
                    content_type=s3_response.get('ContentType', 'application/octet-stream'),
                    timeout=300
                )
            
            logger.info(f"Successfully uploaded to GCS: {gcs_key}")
            return True
            
        except Exception as e:
            logger.error(f"Streaming upload failed: {str(e)}")
            raise
    
    def replicate(self, s3_bucket, s3_key):
        try:
            logger.info(f"Replication request: s3://{s3_bucket}/{s3_key}")
            
            s3_metadata = self._retry_with_backoff(
                self._get_s3_metadata,
                s3_bucket,
                s3_key
            )
            
            logger.info(f"S3 metadata retrieved - Size: {s3_metadata['size']} bytes")
            
            gcs_key = f"{s3_bucket}/{s3_key}"
            
            if self._check_gcs_exists(gcs_key, s3_metadata['etag']):
                return {
                    'status': 'skipped',
                    'message': 'File already exists with same content',
                    'gcs_path': f"gs://{GCS_BUCKET_NAME}/{gcs_key}"
                }
            
            self._retry_with_backoff(
                self._stream_upload,
                s3_bucket,
                s3_key,
                gcs_key,
                s3_metadata
            )
            
            return {
                'status': 'success',
                'message': 'File replicated successfully',
                's3_path': f"s3://{s3_bucket}/{s3_key}",
                'gcs_path': f"gs://{GCS_BUCKET_NAME}/{gcs_key}",
                'size_bytes': s3_metadata['size']
            }
            
        except FileNotFoundError as e:
            logger.error(str(e))
            raise ValueError(str(e))
        except Exception as e:
            logger.error(f"Replication failed: {str(e)}")
            raise


replication_service = ReplicationService()


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 's3-gcs-replication',
        'gcs_bucket': GCS_BUCKET_NAME
    }), 200


@app.route('/v1/replicate', methods=['POST'])
def replicate_endpoint():
    try:
        if not request.is_json:
            return jsonify({
                'error': 'Content-Type must be application/json'
            }), 400
        
        data = request.get_json()
        
        s3_bucket = data.get('s3_bucket')
        s3_key = data.get('s3_key')
        
        if not s3_bucket or not s3_key:
            return jsonify({
                'error': 'Missing required fields: s3_bucket and s3_key'
            }), 400
        
        result = replication_service.replicate(s3_bucket, s3_key)
        
        status_code = 200 if result['status'] == 'success' else 200
        return jsonify(result), status_code
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        logger.error(f"Replication endpoint error: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    if not GCS_BUCKET_NAME:
        logger.error("GCS_BUCKET_NAME environment variable is required")
        exit(1)
    
    port = int(os.getenv('PORT', '5000'))
    logger.info(f"Starting S3 to GCS Replication Service on port {port}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.getenv('DEBUG', 'False').lower() == 'true'
    )