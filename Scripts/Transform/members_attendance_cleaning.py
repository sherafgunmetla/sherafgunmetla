import tabula
import pandas as pd
import warnings
import PyPDF2
import boto3
import re
warnings.filterwarnings('ignore')
import awswrangler as wr
import io
import urllib.parse
from datetime import date

def lambda_handler(event, context):

 
    s3 = boto3.resource('s3')
    #bucket = 'members-attendence-scraped-raw'
    #key = 'Wednesday, 10th July, 2024.pdf'

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    local_file_name = '/tmp/'+key
    s3.Bucket(bucket).download_file(key, local_file_name)
   
    file = open('/tmp/'+ key, 'rb')
    pdf_reader = PyPDF2.PdfReader(file)

    # Get the number of pages in the PDF file
    num_pages = len(pdf_reader.pages)
    keyList = ["Constituency", "Name"]
    d = {}
    for i in keyList:
            d[i] = None
    constituency_list=[]
    name_list=[]
    # Loop through each page in the PDF file
    for page_num in range(num_pages):
        # Get the current page object
        page = pdf_reader.pages[page_num]
        
        # Extract the text from the current page
        page_text = page.extract_text()
        rows = page_text.strip().split('\n')
       
        for i in rows:
            if "NA-" in i: 
                x=i.split(' ')
                constituency_list.append(x[1])
                h = " ".join(x[2:-1])
                name_list.append(h)
            elif "Reserved" in i:
                x=i.split(' ')
                j = " ".join(x[1:3])
                constituency_list.append(j)
                h = " ".join(x[3:-1])
                name_list.append(h)
                
                
    
    dict = {'Constituency': constituency_list, 'Name': name_list} 
    df = pd.DataFrame(dict)
    df['Name'] = df['Name'].replace([' PNATIONAL ASSEMBLY'],'', regex=True)
    
    
    df['Name'] = df['Name'].str.lower()
    df['Name'] = df['Name'].replace(['maj. (r)','chaudhary','ch\\.','mr\\.','chaudhry ','mian\\. ','engr\\. ','ch\\. ','ch ','dr ','dr\\. ','miss ','mr\\. ','ms\\. '],'', regex=True)
    df['Name'] = df['Name'].replace(['-','  '],' ', regex=True)
    df['Name'] = df['Name'].str.strip()
    df['Constituency'] = df['Constituency'].str.strip()
    
    df.loc[((df['Name'] == 'muhammad iqbal khan') & (df['Constituency'] == 'NA-27')), "Name"] = "muhammad iqbal khan (NA-27)"
    df.loc[((df['Name'] == 'muhammad iqbal khan') & (df['Constituency'] == 'NA-235')), "Name"] = "muhammad iqbal khan (NA-235)"
    
    df['Date'] = key[:-4]
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    print(df.head(5))
        
    cleaned_pdf= key[:-4]
    wr.s3.to_csv(df, 's3://members-attendence-scraped-cleaned/'+str(cleaned_pdf)+'-cleaned.csv', index=False,header=True)   

