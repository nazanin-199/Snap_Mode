import uvicorn
from fastapi import FastAPI, File, UploadFile, Body, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import pysolr
from typing import Optional
import base64
from unidecode import unidecode
import bcrypt
from typing import Dict, Any
from datetime import timedelta, datetime

ACCESS_TOKEN_EXPIRE_MINUTES = 30
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



MMERCHANT_ID = "8b12d052-dd2e-4564-ad6f-70a01cd5a90b"  # Required
ZARINPAL_WEBSERVICE = "https://www.zarinpal.com/pg/services/WebGate/wsdl"  # Required
solr_business_user = pysolr.Solr("http://192.168.4.196:8983/solr/Business_Users_Core/",timeout=10,always_commit=True)
solr_change_pass = pysolr.Solr("http://192.168.4.196:8983/solr/Business_Change_Pass_Core/",timeout=10,always_commit=True)
solr_con_logs_core = pysolr.Solr("http://192.168.4.196:8983/solr/Logs_Core", always_commit=True, timeout=10)
solr_con_product_core = pysolr.Solr("http://192.168.4.196:8983/solr/Product_Core", always_commit=True, timeout=10)
solr_con_product_temp_core = pysolr.Solr("http://192.168.4.196:8983/solr/Product_Temp_Core", always_commit=True, timeout=10)
solr_con_product_category_core = pysolr.Solr("http://192.168.4.196:8983/solr/Product_Category_Core",always_commit=True,timeout=10)
solr_con_action_core = pysolr.Solr("http://192.168.4.196:8983/solr/Actions_Core", always_commit=True, timeout=10)

solrSearchCon=pysolr.Solr("http://192.168.4.196:8983/solr/Career_Users_Core", always_commit=True, timeout=10) #connection to core 

@app.post("/api/tim/business/career_login")
async def career_login(data: Dict[Any, Any] = None) :
    national_id =data['national_id']
    userPassword = data['password']
    qStr = 'national_id:' + national_id 
    print(qStr)
    fqStr = ['employee_name:rashidian']
    result = solrSearchCon.search("national_id:" + '"' + national_id + '"', fq=fqStr)
    rowsNo = result.hits
    result = solrSearchCon.search("national_id:" + '"' + national_id + '"', fq=fqStr,start=2, rows=rowsNo, sort='updare_date desc ')
    print(result.hits)
    for res in result:
        password = res['password'][0]
    print(password,date)
    return('sucess')

@app.post("/api/tim/business/list_user")
async def list_user(data: Dict[Any, Any] = None) :
  
    qStr = '*:*'
    print(qStr)
    result = solrSearchCon.search(q=qStr)
    print(result.hits)
    final_results = {}
    data = []
    for res in result:
        user = {}
        user['password'] = res['password'][0]
        user['national_id'] = res['national_id']
        data.append(user)
    final_results["data"] = data
    return(final_results)
