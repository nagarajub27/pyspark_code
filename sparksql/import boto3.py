import boto3
import paramiko
import pymysql
from datetime import datetime
import os


#initialize s3 and sns clients

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

db_connection = pymysql.connect(
    host = 'aurora-db-endpoint',
    user = 'username',
    password = 'password',
    database = 'healthcase_metadata'
)

s3_bucket = 's3://health_data/'
sftp_host = ''
sftp_user = ''
sftp_password = ''
sftp_database = ''
sftp_directory = '/remote/data'

#check for duplicate files
def duplicate_check(file):
    cursor = db_connection.cursor()
    sql_exe = cursor.execute("select * from health_metadata where file_name = '%s'",(file,))
    result = sql_exe.fetchall()
    cursor.close()
    return result

def validate_file_format(file):
    return file.lower().endswith('csv')


def lambda_handler(evnet,context):
    file_name = event['file_name']
    file_path = os.path.join(sftp_directory,file_name)

    #check for duplicates
    if duplicate_check(file_name):
        sns_client.publish(
            TopicArn = 'arn.aws.sns.region.'
        )

    #validate the file format
    if validate_file_format(file):


    #make connection to sftp to download the files
    transport = paramiko.Transport((host,22))
    transport.connect(username = sftp_username,password = sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    #donwload the file from sftp to in memory as object
    file_obj = sftp.open(file_path,'rb')

    #upload the file directly to s3
    s3_client.upload_fileobj(file_obj,Bucket = '',f'')
    file_obj.close()







                             


