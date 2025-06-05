from flask import Flask, request, jsonify, abort
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
EXPECTED_API_KEY = os.getenv("INTERNAL_API_KEY")

# Create DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

table = dynamodb.Table("definitions")

app = Flask(__name__)

@app.route("/save-definition", methods=["POST"])
def save_definition():
    
    api_key = request.headers.get("x-api-key")
    if api_key != EXPECTED_API_KEY:
        abort(401, description="Invalid or missing API key")

    definition = request.json

    table.put_item(Item=definition)

    return jsonify({"message": "Definition saved successfully"}), 201

if __name__ == "__main__":
    app.run(debug=True, port=5000)
