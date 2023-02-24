# Define imports
try:
    import unzip_requirements
except ImportError:
    pass

import json
from io import BytesIO
import time
import os
import boto3
from urllib.parse import unquote_plus
import json
import os
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.visualizers.entitylist import EntityList
from textractor.data.constants import TextractFeatures, Direction, DirectionalFinderType
import re
import os.path

import datetime
from PIL import Image
# def addData_all(role,country,state, lga, ward, pu, file, remark, type, lat, long, phone, email):
import urllib.request
import difflib
import uuid
#Function to extract the features of an audio file


import snowflake.connector


def get_db2():
    return snowflake.connector.connect(

    user= 'hafsa',
    password = 'Hafsa01?',
    account = 'mqnguim-ll14025',
    database='MYTRIAL',
    schema = 'PUBLIC',
    warehouse='TRIAL')


def preprcess_file(image_path):

    queries = [
    "Does Senatorial District word is inside this document ?", 
    "Does House of Representative word is inside this document ?", 
    "Does Presidential word is inside this document ?", 
    "What is the State code value ?",
    "What is the Local Government area code value ?",
    "What is the Registration area code value ?",
    "What is the Polling Unit code value ?",
    "How many number of voters on register?" ,
    "How many number of Acrredited voters ?" ,
    "How many number of ballot papers issued to the polling unit?" ,
    "How many number of unused ballot papers ?" ,
    "How many number of spoiled ballot papers?" ,
    "How many number of used ballot papers ?"  ,
    "How many number of Rejected ballot?"  ,
  


    
]
    # extension = os.path.splitext(image)[1][1:]
    # if extension == 'pdf' or 'PDF':
    #     image = pdf2image
    # urllib.request.urlretrieve(
    #             image,
    #             "image.png")
            
    extractor = Textractor(profile_name="default")
    document = extractor.analyze_document(
    file_source=Image.open(image_path),
    features=[TextractFeatures.QUERIES,TextractFeatures.TABLES],
    queries=queries
)


    table = EntityList(document.tables)
    if len(table) ==2:
        df =  table[1].to_pandas()
    


    party_val = df.iloc[1:][1]
    party_matching = []
    party_real_values = ['A', 'AA', 'AAC', 'ADC', 'ADP', 'APC', 'APGA', 'APM', 'APP', 'BP', 'LP', 'NNPP', 'NRM', 'PDP', 'PRP', 'SDP','YPP', 'ZLP']
    for p in party_val:

        v = difflib.get_close_matches(p, party_real_values)
        
        if len(v) > 0:
            if p =="POP":
                v[0] = "PDP"
            elif p =="AOP":
                v[0] = "ADP"
            elif p =="SOP":
                v[0] ="SDP"
            elif p =="AOC":
                v[0] ="ADC"
            party_matching.append(v[0])

        else:
            pass
    
    number = {}
    total_register = ""
    total_acrredited =""
    total_rejected = ""
    spoilled =""
    valid_votes =""
    used_ballot =""
    ballot_issued = ""
    unused_ballot = ""
    table_name =""
    pucode =""
    for query in document.queries:
        if query.result:

            if query.query ==  'What is the State code value ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value =  value.strip().replace(" ", "")
                # value = int(value)
                state_name =  value

            
            elif query.query ==  'What is the Local Government area code value ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value =  value.strip().replace(" ", "")
                # value = int(value)
                lga_name =  int(value)

            elif query.query ==  'What is the Registration area code value ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value =  value.strip().replace(" ", "")
                # value = int(value)
                ward_name =  int(value)
            

            elif query.query ==  'What is the Polling Unit code value ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value =  value.strip().replace(" ", "")
                # value = int(value)
                pu_name =  int(value)

                pucode += f"PU_CODE = {state_name}/{lga_name}/{ward_name}/{pu_name}"
            elif query.query ==  'Does Senatorial District word is inside this document ?':
                if query.result.answer == "yes":
                    table_name = 'SEN_PU_TABLE'
                
            
            elif query.query ==  'Does House of Representative word is inside this document ?':
                if query.result.answer == "yes":
                    table_name = 'REP_PU_TABLE'
                
            elif query.query ==  'Does Presidential word is inside this document ?':
                if query.result.answer == "yes":
                    table_name = 'PU_RESULT_TABLE'




            elif query.query == 'How many number of voters on register?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                total_register += f"TOTAL_REGISTERED_VOTERS = {value}"
            elif query.query == 'How many number of Acrredited voters ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                total_acrredited += f"TOTAL_ACCREDITED_VOTERS = {value}"
            
            elif query.query == 'How many number of ballot papers issued to the polling unit?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                ballot_issued += f"BALLOT_ISSUED = {value}"
            
            elif query.query == 'How many number of unused ballot papers ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                unused_ballot += f"UNUSED_BALLOT = {value}"
                
            elif query.query == 'How many number of used ballot papers ?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                used_ballot += f"USED_BALLOT = {value}"
            
            elif query.query == '':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                valid_votes += f"VALID_VOTES_C = {value}"
                
            elif query.query == 'How many number of spoiled ballot papers?':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                spoilled += f"SPOILED_BALLOT = {value}"
            
            elif query.query == '"How many number of Rejected ballot?"':
                value= re.sub("\D","",query.result.answer)
                if value =="":
                        value = 0
                value = int(value)

                total_rejected += f"TOTAL_REJECTED_VOTES = {value}"
            else:
                pass

    numberlist= [total_register,total_acrredited,total_rejected,spoilled,valid_votes,used_ballot,ballot_issued,unused_ballot]    
    numberlist = [x for x in numberlist if x != ""]
    numberquery = ", ".join(numberlist)

    values_matching = []
    party_values = df.iloc[1:][2]
    for v in party_values:
        n= re.sub("\D","",v)
        if n =="":
            n = 0
        n = int(n)
        values_matching.append(n)

    party_dictionary =  dict(zip(party_matching,values_matching))
    part = ""
    for key, value in party_dictionary.items():
        part += f"{key} ={value},"
    part = part[:-1]

    country_name= 1
    if table_name == "PU_RESULT_TABLE":
        election = "Presidential elections"
    elif table_name == "SEN_PU_TABLE":
        election = "Senate elections"
    elif table_name == "REP_PU_TABLE":
        election = "House of representatives elections"
    with get_db2() as conn:
        cur = conn.cursor()

        try:
            sql = f"Update {table_name} SET {part},{numberquery},status='collated' where lga_id={lga_name} and ward_id={ward_name} and pu_id = {pu_name}"
            cur.execute(sql)
            sql1 = f"""SELECT * FROM {table_name} where  lga_id = {lga_name} and ward_id = {ward_name} and pu_id= {pu_name}"""
            final ={}
            try:
                cur.execute(sql1)

                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES","BALLOT_ISSUED","UNUSED_BALLOT","SPOILED_BALLOT","VALID_VOTES_C","USED_BALLOT"]
        
                data = ['DATE_TIME', 'PERSON_COLLATED','FILE']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
                
                for key in total:
                    total_results.update( {key:results[0][key]})

                for key in data:
                    other_data_results.update( {key:results[0][key]})

                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                final['message'] = {f"Successfully collated {election} at PollingUnit: {results[0]['PU_NAME']}"}

                return final
            except Exception as e:
                print(e)
                return str(e)
        except Exception as e:
                print(e)
                return str(e)

 
#The lambda_function that calls the other fucntions and do the prediction
def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(key, encoding = 'utf-8')
    tmpkey = key.replace('/', '')
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
    s3_client.download_file(bucket_name, key, download_path)

    response = preprcess_file(download_path)
    
    return {
        'statusCode': 200,
        'headers':{
            'Content-type':'application/json'
        },
        'body': response
    }
    
