# app.py

from flask import Flask, Response, json, request, make_response
import os
import logging
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from flask_cors import CORS


from Mysql import MysqlDatabase
from S3 import create_bucket, add_user_key, create_presigned_post
from flask import json as flask_json
from Util import Response, Validate

db = MysqlDatabase()

# first, load your env file, replacing the path here with your own if it differs
# when using the local database make sure you change your path  to .dev.env, it should work smoothly.
load_dotenv()
# set globals
RDS_HOST = os.environ.get("DB_HOST")
RDS_PORT = int(os.environ.get("DB_PORT", 3306))
NAME = os.environ.get("DB_USERNAME")
PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
PRIMARY_REGION = os.environ.get("PRIMARY_REGION")


# we need to instantiate the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

@app.route("/test")
def hello():
    return "Hello World!"

@app.route('/getUsers', methods=["GET", "POST"])
def getUsers():
    rsp = db.SELECT('getUsers')
    return Response.jsonResponse(rsp)

@app.route('/createUser', methods=["GET", "POST"])
def createUser():
    data = {
        'first_name': request.form.get('first_name'),
        'last_name': request.form.get('last_name'),
        'email': request.form.get('email')
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['first_name', 'last_name', 'email'])
    if not valid:
        error_fields = ', '.join(fields)
        error_message = f"Data missing from these fields: {error_fields}"
        return Response.jsonResponse({"status": "error", "message": error_message}, 400)

    rsp = db.INSERT('createUser', data)
    return Response.jsonResponse(rsp)

@app.route('/cognitoUserToRDS', methods=["POST"])
def cognitoUserToRDS():
    data = {
        'email': request.form.get('email'),
        'email_verified': request.form.get('email_verified'),
        'datestamp': request.form.get('datestamp'),
        'user_pool_id': request.form.get('userPoolId'),
        'user_id': request.form.get('userName'),
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['email', 'email_verified', 'datestamp', 'user_pool_id', 'user_id'])
    if not valid:
        error_fields = ', '.join(fields)
        error_message = f"Data missing from these fields: {error_fields}"
        return Response.jsonResponse({"status": "error", "message": error_message}, 400)

    rsp = db.INSERT('cognitoUserToRDS', data)
    return Response.jsonResponse(rsp)

@app.route('/createCognitoUserKey', methods=["POST"])
def createCognitoUserKey():
    data = {
        'email': request.form.get('email'),
        'email_verified': request.form.get('email_verified'),
        'user_pool_id': request.form.get('userPoolId'),
        'user_id': request.form.get('userName')
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['email', 'email_verified', 'user_pool_id', 'user_id'])
    creation = add_user_key(bucket_name="mgr.users.data", user_id=data['user_id'])
    return str(creation)

@app.route('/getPresignedUserDataUrl', methods=["GET"])
def getPresignedUserDataUrl():
    data = {
    'user_id': request.args.get('user_id'),
    'file_name': request.args.get('file_name')
    }

    bucket_name = 'mgr.users.data'
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    s3_name = data['file_name'].split('.')[0] + timestamp
    s3_name = s3_name.replace(' ', '_')
    s3_name = data['user_id'] + '/' + s3_name

    post_url = create_presigned_post('mgr.users.data', s3_name)
    print(post_url)
    return Response.jsonResponse(post_url)

# include this for local dev

if __name__ == '__main__':
    app.run()
