#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 17:31:07 2024

@author: sherafgun
"""

import json
import requests
import boto3
import time
import awswrangler as wr
import io
from difflib import SequenceMatcher
from datetime import date
import pandas as pd
import numpy as np
import urllib.parse

def lambda_handler(event, context):
    
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

    s3 = boto3.client('s3')
    bucket = 'members-details-scraped-cleaned'

    objs = s3.list_objects_v2(Bucket=bucket)['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified, reverse=True)][0]

    response = s3.get_object(Bucket=bucket, Key=last_added)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(io.StringIO(csv_content))
    #########################################################
    bucket_2 = event['Records'][0]['s3']['bucket']['name']
    key_2 = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
  #  bucket_2 = 'members-attendence-scraped-cleaned'
  #  key_2 = 'Friday, 17th May, 2024-cleaned.csv'
    
    response_2 = s3.get_object(Bucket=bucket_2, Key=key_2)
    csv_content_2 = response_2['Body'].read().decode('utf-8')

    df2 = pd.read_csv(io.StringIO(csv_content_2))
    #########################################################
    
    df_merge = pd.merge(df, df2, on=['Name','Constituency'],how='right')
    df_new = df_merge[df_merge['Party'].isna()][['Name','Constituency']]
    
    member_list = df.Name.unique().tolist()
    details_not_found = df_new.Name.unique().tolist()
    
    dict1 = pd.Series(df.Constituency.values,index=df.Name).to_dict()
    dict2 = pd.Series(df_new.Constituency.values,index=df_new.Name).to_dict()
    #########################################################
      
    def match(m,l):
        equal= 0
        not_equal = 0
        for i in m:
            if i>=80:
                equal=equal+1
            else:
                not_equal=not_equal+1
        x = round((equal/l)*100,0)
        #print(equal)
        #print(x)
        if x>=33:
            return True
        else:
            return False
    
    def list_comparison(list1,list2,c,d):
        match_points=[]
        sum = 0
        length = len(list1)+len(list2) 
        for i in list1:
            for j in list2:
                x = int(SequenceMatcher(a=i, b=j).ratio()*100)
                seq_match = SequenceMatcher(None, c,d)
                ratio = round(seq_match.ratio()*100,0)
                match_points.append(x)
       # print(match_points)
       # print(ratio) 
        if (match(match_points,length) is True and ratio >=70) or ratio >=90:
           return True
        else:
           return False
    #########################################################
    changed = 0
    for key2,value2 in dict2.items():
        for key1,value1 in dict1.items():
            key1_list = key1.split(' ')
            key2_list = key2.split(' ')
            key1_str = ''.join(key1)
            key2_str = ''.join(key2)
            if ('na' in value1.lower() and 'na' in value2.lower() and value1==value2) or ('reser' in value1.lower() and 'reser' in value2.lower()):
                p = list_comparison(key1_list,key2_list,key1_str,key2_str)
                if p is True:  
                    changed = changed+1
                #    print(key1,key2,value1,p,sep='|')
                    details_not_found.remove(key2)
                    df2.loc[df2['Name'] == key2, 'Name'] = key1
    
    df_merge = pd.merge(df, df2, on=['Name','Constituency'],how='inner')
    df2 = df_merge[['Constituency','Name','Date']]
   # print(df2.head(5))
    transformed = key_2.split('-')[0]
    wr.s3.to_csv(df2, 's3://members-attendence-transformed/'+str(transformed)+'-transformed.csv', index=False,header=True)   
    print(changed)
    print(details_not_found)
