import pandas as pd
from pathlib import Path
#import textract
from urllib.request import urlopen
#import tabula
#import PyPDF2
import glob
#import PyPDF4
import numpy as np
import time
import requests
import re
import os
import urllib.request
from bs4 import BeautifulSoup as bs
from requests import get
import warnings
import boto3
warnings.filterwarnings('ignore')

def lambda_handler(event, context):
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
            url = 'https://na.gov.pk/en/attendance2.php'
            response = get(url, headers=headers)
            soup = bs(response.text, 'html.parser')
        except:
            print('Connection forcibly closed by the remote host')
            
   
        links = soup.find_all('a')
            # From all links check for pdf link and
            # if present download file
        filesnames_urls_dict = {}
            
        for link in links:
            i = link.get('href', [])
            text = re.compile(r".*[_][0-9]{3}[.][p][d][f]$")
            if text.match(i):
                title = link.string
                doc = 'https://na.gov.pk/'+str(i[2:])
                filesnames_urls_dict[title] = doc
        
        
        temp_keysList = list(filesnames_urls_dict.keys())
        keysList = []
        for i in temp_keysList:
            if ('February, 2024' not in i) and ('March, 2024' not in i):
                keysList.append(i)
        
        filesnames_urls_dict_final = {}
        for i in filesnames_urls_dict.keys():
            if i in keysList:
                filesnames_urls_dict_final[i] = filesnames_urls_dict[i]
                
        
        for i in filesnames_urls_dict_final:
            
            url = filesnames_urls_dict_final[i]
            r = requests.get(url)
            content = r.content

            with open('/tmp/'+i+'.pdf', 'wb') as fd:
                 fd.write(content)
            bucket = 'members-attendence-scraped-raw'
            client = boto3.client('s3')
            client.upload_file('/tmp/'+i+'.pdf', bucket, i+'.pdf')
      
