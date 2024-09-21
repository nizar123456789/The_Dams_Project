import requests
import pandas as pd
import urllib.parse
import os
import tabula

from datetime import datetime, timedelta

# Get today's date
today = datetime.today()

# Generate a list of dates from today to 30 days earlier
date_list = [(today - timedelta(days=i)).strftime("%d-%m-%y") for i in range(180)]

# Print the list of dates
# Download JSON file that contains pdf names
def extract_data(output_dir):
    headers = {'accept': 'application/json, text/plain, */*',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}

    url = 'http://www.onagri.nat.tn/onagri/barrage'
    r = requests.get(url, headers=headers)
    df = pd.json_normalize(r.json())
    links2 = df['link'].tolist()

    links=[link for link in links2 if "HYDRAULIQUE"not in link]
    cleaned_links = [r.strip() for r in links]
    base_url = "http://www.onagri.nat.tn"
    absolute_path=[urllib.parse.urljoin(base_url,link) for link in cleaned_links[0:4]]
    absolute_urls=absolute_path


    

# Ensure the directory exists
    if not os.path.exists(output_dir):
      os.makedirs(output_dir)

# Download PDFs
    for url in absolute_urls:
       response = requests.get(url)
       if response.status_code == 200:
          file_path = os.path.join(output_dir, os.path.basename(url))
          with open(file_path, 'wb') as f:
              f.write(response.content)
def ingest_data(folder_path):
    extract_data(r"C:\Users\nizar\Desktop\Dams_Project\src\pipeline\PDF_Downloads")
    
    data=pd.DataFrame()

    i=0
# Iterate through all files in the folder
    for filename in os.listdir(folder_path):
       df = tabula.read_pdf(folder_path+'\\' +filename, pages='all')[0]
       df["year"]=df["Unnamed: 1"][2]
     
       data=pd.concat([data,df],axis=0, ignore_index=True)
    return data


#data=ingest_data(r"C:\Users\nizar\Desktop\Dams_Project\src\pipeline\PDF_Downloads")
