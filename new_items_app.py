import datetime
import json
import time
import warnings
from typing import Dict, List , Union  # not known
import numpy as np

import bs4
import pandas as pd
import requests
from copy import deepcopy # not known
from bs4 import BeautifulSoup
from mysql.connector.constants import ClientFlag
from requests.auth import HTTPBasicAuth
from sqlalchemy import false # not known
from sqlalchemy import create_engine
from tqdm import tqdm
import xml.etree.ElementTree as ET
import csv
import tempfile
import datetime as dt
import time

# Set api details
api_key = 'f803b1f2-486e-4de7-9e6c-faa45366bb28'
username = "Andrew Mensah-Onumah"
password = "M@verick12345"

# Connection to MySQL Engine
my_conn = create_engine("mysql+mysqldb://root:T0pGunMaver123@localhost/maverick_db")
# ----------------------------------------------------------------------------------------------------------------------------------------
def data_formatting(surveyIDs): # consider changing this argument
        '''
        Gets response text from API pull and removes unwanted punctuations. 
        Format response text|| Data Cleaning steps
        '''

        surveyIDs = surveyIDs.replace('[', '')
        surveyIDs = surveyIDs.replace(']', '')
        surveyIDs = surveyIDs.replace('\r', '')
        surveyIDs = surveyIDs.replace('\n', '')
        surveyIDs = surveyIDs.strip()
        surveyIDs = surveyIDs.replace(' ', '')
        surveyIDs = surveyIDs.split(',')
        return surveyIDs
# ------------------------------------------------------------------------------------------------------------------------------------------

def get_store_id():
    """
    Receive survey id and Extract subject_ids for a given day period
    In this code I made start date and end date the same. It can be modified as suited
    """
    global start_date
    start_date = end_date = (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d") # correct this
    print(start_date)
    # set api key
    api_key = 'f803b1f2-486e-4de7-9e6c-faa45366bb28'
    url = f"http://api.dooblo.net/newapi/SurveyInterviewIDs?surveyIDs={survey_id}&testMode=False&completed=True&filtered=False&dateStart={start_date}T00%3a00%3a00.0000000%2b00%3a00&dateEnd={end_date}T23%3a59%3a59.9990000%2b00%3a00&dateType=Upload"
    payload = {}
    headers = {
        'Cookie': 'ASP.NET_SessionId=fqtuuiimuc0ij43ejti02ktu'
    }
    response = requests.request("GET", url, headers=headers, data=payload, auth=HTTPBasicAuth(
        f"{api_key}/{username}", f"{password}"))
    print('HTTP Request response completed')
    surveyIDs = response.text
    list_subjects = (data_formatting(surveyIDs))
    print(f"Number of subject ids: {len(list_subjects)}") # store extracted subject_ids in a list
    print("-"*40)
    return list_subjects  

# -----------------------------------------------------------------------------------------------------------------------------------------
def download_xml(subj_id, survey_id, api_key, username, password):
    """
    Receive Subject_id along with survey_id. Use these two to extract data pertaining to a shop
    Sample data extraction with SurveyInterviewData
    """    
    url = f"https://api.dooblo.net/newapi/SurveyInterviewData?subjectIDs={subj_id}&surveyID={survey_id}&onlyHeaders=false&includeNulls=false"
    payload = {}
    headers = {
        'Cookie': 'ASP.NET_SessionId=fqtuuiimuc0ij43ejti02ktu',
        'Accept': 'text/xml',
        'accept-encoding': 'UTF-8'
    }
    response = requests.request("GET", url, headers=headers, data=payload, auth=HTTPBasicAuth(
        f"{api_key}/{username}", f"{password}"))
    return response

# ------------------------------------------------------------------------------------------------------------------------------------------
def xml_to_list(response):
    """
    Load and parsing of files
    """
    # Initialize lists to store data
    data = []

    # Parse the XML content directly from the response text
    root = ET.fromstring(response.text)

    # Extract data from XML tree
    for element in root.iter():
        if element.text and element.text.strip():
            data.append([element.tag, "text", element.text])
        for attribute, value in element.attrib.items():
            data.append([element.tag, attribute, value])
    return data

# ------------------------------------------------------------------------------------------------------------------------------------------
def data_transform(subj_id, survey_id, api_key, username, password):
    """
    Extract the values you need
    Load csv file into dataframe and then select responses in the FullVariable, QuestionAnswer sections
    Create a DataFrame from the extracted data
    """
    try:
        response = download_xml(subj_id, survey_id, api_key, username, password)
        data_list = xml_to_list(response)
        df = pd.DataFrame(data_list, columns=["Element", "Attribute", "Value"])
        df_extract = df[df['Element'].isin(['FullVariable','QuestionAnswer','SubjectNum','Upload'])] # the loaded csv file has columns Element, Attribute and Value. In the element column, for items, we pick the listed variables of interest ie. fullvariable, questionanswer,...
        df_result_extract = df_extract[df_extract['Attribute'].isin(['text'])].reset_index(drop=True)[['Element','Value']] # picks selected data points with Attribute as 'Text' only, resets the index to start from 0 and then drops other columns leaving Element and Value

        # Pick Subject number, Upload date for now...any other special column can come here
        store_info = df_result_extract.iloc[-2:].transpose().reset_index(drop=True) # picks subject number and upload date
        store_info = store_info.rename(columns=store_info.iloc[0]).drop(store_info.index[0]) # remove headers
        store_info.reset_index()
      
        # New items
        # Extracting the Outlet code
        # Had to create two instances for how api returns outlet code and outlet name
        df_one = pd.DataFrame()
        if not df_result_extract.empty:
            try:
                df_one_main = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code_'][0]:df_result_extract.index[df_result_extract['Value'] == 'OutletName'][0], :]
            except IndexError:
                print("Subsetting 1 failed!")
                try:
                    df_one_annex = df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'Outlet_Name'][0]:df_result_extract.index[df_result_extract['Value'] == 'Outlet_Code'][0]+1, :]
                except IndexError:
                    print("Subsetting 2 failed!")
                else:
                    if not df_one_annex.empty:
                        df_one = df_one_annex.reset_index(drop=True)
                        df_one = df_one.iloc[[2,3],:]
            else:
                if not df_one_main.empty:
                    df_one = df_one_main.reset_index(drop=True)
                    df_one = df_one.iloc[[0,1],:]
                else:
                    print("Subsetting 1 assignment failed!")
        else:
            print("The dataframe is empty")
        
        store_id = df_one['Value'].to_frame().reset_index(drop=True)
        store_id = store_id.rename(columns=store_id.iloc[0]).drop(store_id.index[0]) # remove headers
        store_id.reset_index()

        # Extracting info on items in the store
        df_two = df_result_extract['Value'].to_frame()
        df_items= df_result_extract.loc[df_result_extract.index[df_result_extract['Value'] == 'I_1_Export_Category'][0]:, :] # select from export_category to Export_price
        df_items_use = df_items.iloc[:-8]['Value'].to_frame().reset_index(drop=True)

        # Merge outlet code with subject number and upload date for use
        df_store_details = pd.concat([store_info, store_id], axis=1)

        ## Sorting out item details, rearranging them to all exist in one column per variable type
        transformed_data = {} # Instantiate empty dict

        # # Loop through the DataFrame in steps of 2
        for i in range(0, len(df_items_use), 2):
            # Use the first value as the header and the second value as the value beneath the header
            header = df_items_use.iloc[i, 0]
            value = df_items_use.iloc[i+1, 0]
            # Add the header and value to the dictionary
            transformed_data[header] = value
        df_sample = pd.DataFrame([transformed_data])

        #Manipulating resulting dataframe
        df_sample.columns = df_sample.columns.str.replace('I_\d+_', '', regex=True) 
        melted_df = df_sample.melt()

        # # Create a new column to group the rows by
        melted_df['Group'] = (melted_df['variable'] == 'Export_Category').cumsum()

        # Pivot the DataFrame to transform the "variable" column into columns
        melted_df = melted_df.pivot_table(index='Group', columns='variable', values='value', aggfunc=sum) # aggfunc here is to deal with entry columns with duplicates, can't tell where duplicate even is ie. specific column!!! serious attention
        # Reset the index to make "Group" a regular column
        melted_df.reset_index(inplace=True)
        melted_df_edit = melted_df.drop(columns=['Group'], axis=0)
        # Re-order columns
        melted_df_final = melted_df_edit.iloc[:,[4,3,6,1,12,11,8,2,14,9,7,15,13,5,0,10]]

        # Finally, put all details together
        df_items_store_details = pd.concat([df_store_details, melted_df_final], axis=1)

        ## Fill in remaining rows of missing values
        # Select which outlet code to use
        if 'Outlet_Code_' in df_items_store_details.columns: 
            df_items_store_details['Outlet_Code_'] = df_items_store_details['Outlet_Code_'].fillna(df_items_store_details['Outlet_Code_'].iloc[0])
        elif 'Outlet_Code' in df_items_store_details.columns:
            df_items_store_details['Outlet_Code'] = df_items_store_details['Outlet_Code'].fillna(df_items_store_details['Outlet_Code'].iloc[0])
        df_items_store_details['Upload'] = df_items_store_details['Upload'].fillna(df_items_store_details['Upload'].iloc[0])
        df_items_store_details['SubjectNum'] = df_items_store_details['SubjectNum'].fillna(df_items_store_details['SubjectNum'].iloc[0])
    except IndexError:
        print("No new items found.")
    else:
        return df_items_store_details
### ---------------------------------------------------------------------------------------------------------------------------------------
### ---------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Begin timer
    start = time.time()

    # Define lists for use
    new_final_df = [] # store processed info of all stores to produce a bigger df of outlets

    # Begin iteration
    try:
        survey_id = input('Enter survey id: ') # input survey id
        new_list = get_store_id() # get store ids for given period, returns a list of store ids
        for outlet in new_list: # iterate through ids, transform their data and append them to a big list
            dataframe_return = data_transform(outlet, survey_id, api_key, username, password)
            new_final_df.append(dataframe_return)
        
        # Save processed stores data
        merged_df = pd.concat(new_final_df, axis=0)
        merged_df.insert(0, 'Period', dt.datetime.today().replace(day=1).date().strftime('%Y-%m-%d')) # Add 'period' column
        merged_df.to_excel(f'new_items_{start_date}.xlsx', index=False)
        merged_df.to_sql(con=my_conn, name='new_items', if_exists='replace', index=False)
    except KeyError:
        print("No subject ids for the day!")       

    # End timer
    end = time.time()
    print("-"*40)
    print(f"Program run successfully. It took {round((end - start)/60, 2)} minutes to run.")
