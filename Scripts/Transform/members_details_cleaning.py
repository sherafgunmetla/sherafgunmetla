import json
import requests
import boto3
import time
import awswrangler as wr
import io
from datetime import date
import pandas as pd
import numpy as np
import urllib.parse


def lambda_handler(event, context):
        
    s3 = boto3.client('s3')
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
   
   # bucket = 'members-details-scraped-raw'
  #  key = '2024-09-25.csv'
    
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(io.StringIO(csv_content))

    # Cleaning #

    df[['Constituency','City']] = df['Constituency'].str.split('(',expand=True)
   
    df['City'] = df['City'].str.split('-').str[0]
    df.City = df.City.fillna('Reserved Seat')
 
    df['City'] = df['City'].replace(['\)'],'', regex=True)
    df['Name'] = df['Name'].str.lower()
    df['Name'] = df['Name'].replace(['maj. (r)','chaudhary','ch\\.','mr\\.','chaudhry ','mian\\. ','engr\\. ','ch\\. ','ch ','dr ','dr\\. ','miss ','mr\\. ','ms\\. '],'', regex=True)
    df['Name'] = df['Name'].replace(['-','  '],' ', regex=True)
    df['Name'] = df['Name'].str.strip()
    df['Constituency'] = df['Constituency'].str.strip()
    
 
    df.loc[((df['Name'] == 'abdul aleem khan') & (df['Constituency'] == 'NA-117')), "Name"] = "abdul aleem khan (NA-117)"
    df.loc[((df['Name'] == 'abdul aleem khan') & (df['Constituency'] == 'NA-219')), "Name"] = "abdul aleem khan (NA-219)"
    
    df.loc[((df['Name'] == 'muhammad iqbal khan') & (df['Constituency'] == 'NA-27')), "Name"] = "muhammad iqbal khan (NA-27)"
    df.loc[((df['Name'] == 'muhammad iqbal khan') & (df['Constituency'] == 'NA-235')), "Name"] = "muhammad iqbal khan (NA-235)"
    
    df['Constituency Type'] = np.where(df['Constituency'].str.contains('Reserved'), df.Constituency, 'General')
    df['Constituency'] = np.where(df['Constituency'].str.contains('Reserved'), 'Reserved Seat', df.Constituency)
    
    cleaned_csv= key[:-4]
    

    wr.s3.to_csv(df, 's3://members-details-scraped-cleaned/'+str(cleaned_csv)+'-cleaned.csv', index=False,header=True)   
        

    
           