#This script will help your business to track all the customer data that your VTEX platform storage, in this case we are going to extract some customers information
#This is the Cloud Function that you are going to execute every X hours

#All the libraries that we are going to use
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
import json
import os

"""
In case you want to run this Python Script in your PC use the credentials of your service account of GCP
credentials_path = "C:/Python/your_project_file_from_GCP.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
"""

#Calling the BQ's API
client = bigquery.Client()

#Variables

#Defining the dates to extract the data
Initial_Date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
End_Date = (datetime.today() + timedelta(days=0)).strftime('%Y-%m-%d')

Date_Range = "createdIn%20between%20"+ str(Initial_Date) + "%20and%20" + str(End_Date)

#VTEX Parameters
accountName  = "your_business" #*
environment  = "myvtex"
VTEX_API_App_key = "your_api_key" #*
VTEX_API_App_token = "your_api_token" #*
data_entity = "CL" 
fields_to_extract = "userId,email,gender,birthDate,createdIn" #Example

#BQ Parameters
write_disposition_method = "WRITE_APPEND" #*"WRITE_APPEND" or "WRITE_TRUNCATE"
bq_table_id = 'your_BQ_project.your_BQ_dataset.your_BQ_table_ID' #*

#API function that extract all the fields that we specify above, this part of the script saves the recursive token and first thousands of customers
def getToken():
    url = "https://" + str(accountName) + "." + str(environment) + ".com/api/dataentities/" + str(data_entity) + "/scroll?_size=1000&_fields=" + str(fields_to_extract) + "&_where=" + str(Date_Range)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.vtex.ds.v10+json",
        "X-VTEX-API-AppKey": VTEX_API_App_key,
        "X-VTEX-API-AppToken": VTEX_API_App_token
    }
    response = requests.request("GET", url, headers=headers)
    df_token = response.headers['X-VTEX-MD-TOKEN']
    df_document = pd.DataFrame(response.json())
    df_document['date_record'] = datetime.today()
    df_document['process_log'] = "CF-vtex-script-automation-master-customers"
    return df_token, df_document

Token, df_documentId = getToken()

#API function that scrolls all the remaining customers
def getDocuments():
    url = "https://" + str(accountName) + "." + str(environment) + ".com/api/dataentities/" + str(data_entity) + "/scroll?_token=" + str(Token)
    headers = {
            "Content-Type": "application/json",
            "Accept": "application/vnd.vtex.ds.v10+json",
            "X-VTEX-API-AppKey": VTEX_API_App_key,
            "X-VTEX-API-AppToken": VTEX_API_App_token
    }
    response = requests.request("GET", url, headers=headers)
    df_documentId = pd.DataFrame(response.json())
    df_documentId['date_record'] = datetime.today()
    df_documentId['process_log'] = "CF-vtex-script-automation-master-customers"
    return df_documentId

#Storing all customers in a dataframe
def totalDocuments():
    aux = pd.DataFrame()
    df_totaldocs = df_documentId
    i = 0

    try:
        for i in range(1500):
            aux = getDocuments()
            df_totaldocs = pd.concat([df_totaldocs, aux])
            i = i + 1

    except:
        print("NA")
        i = i + 1

    return df_totaldocs

#Job that upload the dataframe into BQ
def loadDocument(df, tabla_id):
    write_disposition = write_disposition_method
    job_config = bigquery.LoadJobConfig(
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition=write_disposition,
    )

    job = client.load_table_from_dataframe(
        df, tabla_id, job_config=job_config
    )  # Make an API request.
    job.result()


def start(a):
    table_id = bq_table_id
    df = totalDocuments()
    docs = pd.DataFrame()
    docs = pd.concat([docs, df])

    loadDocument(docs, table_id)

    return "ok"

#Executing the workflow !
start("a")


#df_documentId.to_csv(r"C:\Users\Eduardo Huiza\Desktop\docu.csv")
