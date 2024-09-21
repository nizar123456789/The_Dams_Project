from data_ingestion import ingest_data
import pandas as pd
from azure.storage.blob import BlobServiceClient
import re
from upload import upload_file_to_cloud
import warnings
from upload import read_file_from_cloud
warnings.filterwarnings("ignore")


def convert_to_float(ch):
  ch=re.sub(r'[a-zA-Z]', '', ch)
  ch=ch.replace('.','')

  if '+'in ch:
    l=ch.split('+')
    return float(int(l[0])+int(l[1]))
  else:
    l=ch.split()
    ch2="".join(l)
    ch2=ch2.replace(',','.')
    return float(ch2)
  
def transform_data(data):
    data=ingest_data(r"C:\Users\nizar\Desktop\Dams_Project\src\pipeline\PDF_Downloads")
    data=data.drop(index=[0,1,2])
    data=data.drop(columns=["Unnamed: 5","Unnamed: 8","Unnamed: 9","Unnamed: 4"],axis=1)
    data["average_revenue_per_month"]=data["APP0RTS"].str.split().str[0]
#data=data.drop(columns=["الايرادات"],axis=1)
    data["release_per_day"]=data["LES LACHERSالسحوبات"].str.split().str[0]
    data["releases_per_day_lastYear"]=data["LES LACHERSالسحوبات"].str.split().str[1]
    data["releases_cumulated_per_day"]=data["LES LACHERSالسحوبات"].str.split().str[2]
    data["storage"]=data["المخزون بالسدود"].str.split().str[0]
    data["storgae_last_year"]=data["المخزون بالسدود"].str.split().str[1]
    data=data.drop(columns=["APP0RTS","LES LACHERSالسحوبات","المخزون بالسدود"],axis=1)
    data=data.rename(columns={"Unnamed: 0":"Dams","Unnamed: 1":"Revenue_per_day","Unnamed: 2":"Revenue_per_month",
                        "الايرادات":"Revenue_in_last_year","Unnamed: 3":"periodic_average_Revenue",
                        "Unnamed: 6":"release_last_year","Unnamed: 7":"filling_percentage",
                        })
    
    df=data.copy()
    df=df.drop([43,44,45])
    data_set=pd.DataFrame()
    data_set["Dams"]=df["Dams"]
    data_set["Revenue_per_day"]=df["Revenue_per_day"]
    data_set["Revenue_per_month"]=df["Revenue_per_month"]
    data_set["Revenue_in_last_year"]=df["Revenue_in_last_year"]
    data_set["release_last_year"]=df["release_last_year"]
    data_set["filling_percentage"]=df["filling_percentage"]
    data_set["year"]=df["year"]
    data_set["average_revenue_per_month"]=df["average_revenue_per_month"]
    data_set["release_per_day"]=df["release_per_day"]
    data_set["releases_per_day_lastYear"]=df["releases_per_day_lastYear"]
    data_set["releases_cumulated_per_day"]=df["releases_cumulated_per_day"]
    data_set["storage"]=df["storage"]
    data_set["storgae_last_year"]=df["storgae_last_year"]
    data_set=data_set.dropna()
    
    #Apply the convert to float function 
    data_set["Revenue_per_day"]=data_set["Revenue_per_day"].apply(convert_to_float)
    data_set["Revenue_per_month"]=data_set["Revenue_per_month"].apply(convert_to_float)
    
    data_set["Revenue_in_last_year"]=data_set["Revenue_in_last_year"].apply(convert_to_float)
    data_set["release_last_year"]=data_set["release_last_year"].apply(convert_to_float)
    
    data_set["filling_percentage"]=data_set["filling_percentage"].apply(convert_to_float)
    data_set["average_revenue_per_month"]=data_set["average_revenue_per_month"].apply(convert_to_float)
    data_set["release_per_day"]=data_set["release_per_day"].apply(convert_to_float)
    data_set["releases_per_day_lastYear"]=data_set["releases_per_day_lastYear"].apply(convert_to_float)
    data_set["releases_cumulated_per_day"]=data_set["releases_cumulated_per_day"].apply(convert_to_float)
    data_set["storage"]=data_set["storage"].apply(convert_to_float)
    data_set["storgae_last_year"]=data_set["storgae_last_year"].apply(convert_to_float)
    print(data_set.info())
    df2=data_set.copy()
    l=df2.columns
    for i in l:
      if df2[i].dtypes!='O':
        df2[i]=df2[i].apply(int)
    final_data=df2[~df['Dams'].str.contains("TOTAL", na=False) & ~df2['Dams'].str.contains("TOTAL GENERAL", na=False)&~df2['Dams'].str.contains("S /T", na=False)&~df2['Dams'].str.contains("S/T CAP-BON", na=False)&~df2['Dams'].str.contains("S /T Nord", na=False)]
    
    return final_data
    
data=read_file_from_cloud()

print(data.columns)
