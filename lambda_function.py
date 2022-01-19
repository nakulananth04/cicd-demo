#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import boto3
import io
import requests
import json
from io import StringIO
def lambda_handler(event, context):
    s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = 'sourcebucketdemo1';
    s3 = boto3.client('s3', aws_access_key_id='AKIAZRRIY4NWWO6V7QBS',  aws_secret_access_key='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')
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()));

    service_name = 's3'
    region_name = 'ap-south-1'
    aws_access_key_id = 'AKIAZRRIY4NWWO6V7QBS'
    aws_secret_access_key = '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'
    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket='targetbucketdemo1';
    df = initial_df[(initial_df.type == "Movie")];
    df1 = df.loc[:, ~df.columns.isin(['date_added', 'description', 'duration'])];
    csv_buffer = StringIO()
    df1.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())

    bucket='imdb01';
    dummy1 = pd.DataFrame()
    for i in df['title']:
        web = 'https://www.omdbapi.com/?t=' + i + '&apikey=9b925aaa'
        response_API = requests.get(web)
        h = response_API.text
        t = json.loads(h)
        # file = pd.DataFrame((t))
        dummy = pd.json_normalize(t)
        dummy1 = dummy1.append(dummy)
    csv_buffer1 = StringIO()
    dummy1.to_csv(csv_buffer, index=False);
    s3_resource.Object(bucket1, s3_file_key).put(Body=csv_buffer1.getvalue())

    """
    bucket1 = 'imdb01';
    all_titles = df1['title']
    #tv_titles_head = all_titles.head()
    for i in all_titles:
        web = 'https://www.omdbapi.com/?t=' + str(i) + '&apikey=caab7032'
        response_API = requests.get(web)
        api_text = response_API.text
        json_text = json.loads(api_text)
        # print(json_text)
        final = pd.json_normalize(json_text)
        #print(final)
        # final.to_csv("existing_tv.csv", mode="a", index=False, header=False)
        csv_buffer = StringIO()
        final.to_csv(csv_buffer, index=False);
        s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())
    """