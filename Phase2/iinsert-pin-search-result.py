import csv
import json
import urllib3
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import pysolr
from typing import Optional
import base64
from unidecode import unidecode
import bcrypt
from typing import Dict, Any
from datetime import timedelta, datetime
import requests

urllib3.disable_warnings()
solrPinCore=pysolr.Solr("http://localhost:8983/solr/Pinterest_core", always_commit=True, timeout=10) #connection to Pinterest_core
url_str='https://www.pinterest.com/resource/BaseSearchResource/get/?source_url=/search/pins/?q=word_search&rs=typed&term_meta[]=word_search' #base address

with open('/home/ahmadpour/Documents/AnyDesk/word_search.csv' , 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        searchWord=row[0]
        main_url=url_str.replace('word_search',searchWord) 
        retry_strategy = Retry(
                            total=20,
                            backoff_factor=8,
                                )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        adapter.max_retries.respect_retry_after_header = False
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        main_response = http.get(main_url, verify=False ,timeout=30)
        filename =main_response.text
        dict1 = {}
        # creating dictionary
        with open(filename) as fh:
        
            for line in fh:
        
                # reads each line and trims of extra the spaces 
                # and gives only the valid words
                command, description = line.strip().split(None, 1)
                dict1[command] = description.strip()
                
        out_file = open("main_response.json", "w")
        #json.dump(dict1, out_file, indent = 4, sort_keys = False)
        out_file.close()
        j = json.loads(main_response.json)

        for i in range(1, len(j["resource_response"]["data"]["results"])):

            pin_id = j["resource_response"]["data"]["results"][i][id]

            img_url = j["resource_response"]["data"]["results"][i]["images"]["orig"]["url"]

            solrPinCore.add([{
                "pin_id": pin_id,
                "date":{datetime.now().isoformat(timespec="seconds") + "Z"},
                "image_url": img_url,
                "search_word": searchWord,
                "status": 'initialized'
            }])
