# app.py

from flask import Flask, Response, json, request, make_response
import os
import logging
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
from flask_cors import CORS
import re


from Mysql import MysqlDatabase
from S3 import create_bucket, add_user_key, create_presigned_post, list_bucket_objects, list_bucket_objects_v2
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
    valid, fields = Validate.validateRequestData(data, required_fields=['user_id', 'file_name'])

    bucket_name = 'mgr.users.data'
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    s3_name = data['file_name'].split('.')[0] + timestamp
    s3_name = s3_name.replace(' ', '_')
    s3_name = data['user_id'] + '/' + s3_name + '.' + data['file_name'].split('.')[1]
    print(s3_name)

    post_url = create_presigned_post('mgr.users.data', s3_name)
    print(post_url)
    return Response.jsonResponse(post_url)

@app.route('/getDataByUser', methods=["GET"])
def getDataByUser():
    data = {
    'user_id': request.args.get('user_id'),
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['user_id'])
    bucket_name = 'mgr.users.data'
    user_id = data['user_id'] + '/'
    rsp = list_bucket_objects(bucket_name=bucket_name, prefix=user_id)
    reg='\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}'

    objects = [{'key':''.join(re.split(reg, x['Key'].split('/')[1])), 'size':x['Size'], 'mod':x['LastModified']} for x in rsp['Contents']]

    return Response.jsonResponse(objects)

@app.route('/listDataByUser', methods=["GET"])
def listDataByUser():
    data = {
    'user_id': request.args.get('user_id'),
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['user_id', 'key'])
    bucket_name = 'mgr.users.data'
    prefix = data['user_id'] + '/'
    rsp = list_bucket_objects(bucket_name=bucket_name, prefix=prefix)

    objects = [{'key': x['Key'].split('/')[1], 'size':x['Size'], 'mod':x['LastModified']} for x in rsp['Contents']]

    return Response.jsonResponse(objects)

@app.route('/getPresignedUserChartUrl', methods=["GET"])
def getPresignedUserChartUrl():
    data = {
    'user_id': request.args.get('user_id'),
    'file_name': request.args.get('file_name')
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['user_id', 'file_name'])

    bucket_name = 'mgr.users.data'
    prefix = 'charts'
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    if "." in data['file_name']:
        s3_name = data['file_name'].split('.')[0] + timestamp
        s3_name = s3_name.replace(' ', '_')
        s3_name = data['user_id'] + '/' + prefix +'/' + s3_name + '.' + data['file_name'].split('.')[1]
    else:
        s3_name = data['file_name'] + timestamp
        s3_name = s3_name.replace(' ', '_')
        s3_name = data['user_id'] + '/' + prefix +'/' + s3_name + '.json'

    print(s3_name)

    post_url = create_presigned_post('mgr.users.data', s3_name)
    print(post_url)
    return Response.jsonResponse(post_url)
@app.route('/getChartsByUser', methods=["GET"])
def getChartsByUser():
    data = {
    'user_id': request.args.get('user_id'),
    }
    valid, fields = Validate.validateRequestData(data, required_fields=['user_id'])
    bucket_name = 'mgr.users.data'
    user_id = data['user_id'] + '/charts/'
    rsp = list_bucket_objects_v2(bucket_name=bucket_name, prefix=user_id)

    # objects = [{'key': x['Key'].split('/')[1], 'size':x['Size'], 'mod':x['LastModified']} for x in rsp['Contents']]
    # print(objects)
    return Response.jsonResponse(rsp)

# include this for local dev

if __name__ == '__main__':
    app.run(debug=True)
