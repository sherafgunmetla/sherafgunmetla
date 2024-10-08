import pandas as pd
from datetime import date
import awswrangler as wr
import numpy as np
import time
import boto3
import re
from bs4 import BeautifulSoup as bs
from requests import get

def lambda_handler(event, context):
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
        url = 'https://na.gov.pk/en/all_members.php'
        response = get(url, headers=headers)
        soup = bs(response.text, 'html.parser')
    except:
        print('Connection forcibly closed by the remote host')
        
        
    df = pd.DataFrame()
    urls=[]
    for a in soup.find_all('a', href=True):
        href = a['href']
        if re.search('profile.php\?uid', href):
            if href not in urls:
                urls.append(href)
                
    keyList = ['Name','Province','Constituency','Party','Oath Taking Date']
    d = {}
    for i in keyList:
        d[i] = []
        
    def general_seats(ss,ee):
            # General Seats
        for i in range(ss,ee):   
            list=[]
          # time.sleep(10)
            try:
                url = 'https://na.gov.pk/en/'+urls[i]
              #  print(url)
                response = get(url, headers=headers)
                soup = bs(response.text, 'html.parser')
                table = soup.find("table" , {"class": "profile_tbl table-bordered"})
                rows = table.find_all('tr')
                for i in rows:
                        row_data_header= i.find('th')
                        row_header = [i.text for i in row_data_header][0].strip()
                        list.append(row_header)
                for i in keyList:
                    if i in list:
                        for k in rows:
                            row_data_header= k.find('th')
                            row_header = [i.text for i in row_data_header][0].strip()
                            if i==row_header:
                                row_data= k.find('td')
                                row = [i.text for i in row_data][0].strip()
                                d[i].append(row)
                    else:
                        d[i].append('not found')
                        
            except:
                        
                print("Exception for : "+i)
                
                
    def reserved_for_women(ss,ee):
    # Reserved for Women
        for i in range(ss,ee):
            list=[]
            try:
              # time.sleep(10)
                url = 'https://na.gov.pk/en/'+urls[i]
                response = get(url, headers=headers)
                soup = bs(response.text, 'html.parser')
                table = soup.find("table" , {"class": "profile_tbl table-bordered"})
                rows = table.find_all('tr')
                for i in rows:
                        row_data_header= i.find('th')
                        row_header = [i.text for i in row_data_header][0].strip()
                        list.append(row_header)
                for i in keyList:
                    if i in list:
                        for k in rows:
                            row_data_header= k.find('th')
                            row_header = [i.text for i in row_data_header][0].strip()
                            if i==row_header:
                                row_data= k.find('td')
                                row = [i.text for i in row_data][0].strip()
                                d[i].append(row)
                    else:
                        d[i].append('Reserved for Women')
                        
            except:
                        
                print("Exception for : "+i)             
                
                
    def reserved_for_minorities(ss,ee):
            # Reserved for Minorities
        for i in range(ss,ee):   
            list=[]
            try:
              # time.sleep(10)
                url = 'https://na.gov.pk/en/'+urls[i]
                response = get(url, headers=headers)
                soup = bs(response.text, 'html.parser')
                table = soup.find("table" , {"class": "profile_tbl table-bordered"})
                rows = table.find_all('tr')
                for i in rows:
                        row_data_header= i.find('th')
                        row_header = [i.text for i in row_data_header][0].strip()
                        list.append(row_header)
                for i in keyList:
                    if i in list:
                        for k in rows:
                            row_data_header= k.find('th')
                            row_header = [i.text for i in row_data_header][0].strip()
                            if i==row_header:
                                row_data= k.find('td')
                                row = [i.text for i in row_data][0].strip()
                                d[i].append(row)
                    else:
                        d[i].append('Reserved for Minorities')
                        
            except:
                        
                print("Exception for : "+i)
                
    
    today = date.today()
    general_seats(0,266)
    time.sleep(10)
    reserved_for_women(266,306)
    time.sleep(10)
    reserved_for_minorities(306,313) 
    df = pd.DataFrame(d)    
   
    df.loc[df["Name"] == "Sardar Ayaz Sadiq, Speaker National Assembly", "Name"] = 'Sardar Ayaz Sadiq'
    df.loc[df["Name"] == "Syed Ghulam Mustafa Shah, Deputy Speaker National Assembly", "Name"] = 'Syed Ghulam Mustafa Shah'
    


    df['Oath Taking Date'] = pd.to_datetime(df['Oath Taking Date'])
    df['Oath Taking Date'] = df['Oath Taking Date'].dt.strftime('%Y-%m-%d')

    wr.s3.to_csv(df, 's3://members-details-scraped-raw/'+str(today)+'.csv', index=False,header=True)   
