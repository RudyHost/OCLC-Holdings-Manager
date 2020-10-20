import requests
import json
import os
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import time
from urllib.parse import quote
import uuid

class OhmOclc:

    def divide_chunks(self, l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def token_updater(self, new_token):
        self.token = new_token
        return None

    def oclc_login(self):
        scopes = ['WorldCatMetadataAPI', 'refresh_token']
        token_url = 'https://oauth.oclc.org/token'
        extra = {
            'client_id': self.client_id,
        }

        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        client = BackendApplicationClient(client_id=self.client_id, scopes=scopes, auto_refresh_url=token_url, auto_refresh_kwargs=extra, token_updater=self.token_updater)

        self.session = OAuth2Session(client=client,)
        self.session.fetch_token(token_url=token_url, auth=auth, scope=scopes, include_client_id=True, )


    def test_wskey(self, holding_map):

        if self.session == None:
            self.oclc_login()

        symbols = set()
        for i, (ils, oclc) in enumerate(holding_map.items()):
            symbols.add(oclc)

        failed_symbols = dict()

        for symbol in symbols:
            add_url = f"https://worldcat.org/ih/institutionlist?instSymbols={quote(symbol)}&oclcNumber=953197097"
            add = self.session.post(url=add_url, headers=self.headers)
            response = json.loads(add.text)
            if not add.ok:
                failed_symbols[symbol] = response['message']
            #self.session.close()

        time.sleep(10)

        for symbol in symbols:
            delete_url = f"https://worldcat.org/ih/institutionlist?instSymbols={quote(symbol)}&oclcNumber=953197097&cascade=1"
            delete = self.session.delete(url=delete_url, headers=self.headers)
            response = json.loads(delete.text)
            if not delete.ok:
                failed_symbols[symbol] = response['message']
            else:
                response = json.loads(delete.text)
                if response["entry"][0]["httpStatusCode"] != "HTTP 200 OK":
                    failed_symbols[symbol] = response['message']
            #self.session.close()
        self.session.close()
        
        return failed_symbols
    
    def unset_holding(self, oclc_number, symbols):

        divided_symbols = list(self.divide_chunks(symbols, 50))

        for library_symbols in divided_symbols:
            url_symbols = ','.join(library_symbols)
            url = f"https://worldcat.org/ih/institutionlist?instSymbols={quote(url_symbols,safe='/,')}&oclcNumber={oclc_number}&cascade=1"
            delete = self.session.delete(url=url, headers=self.headers)
            file_name = f"results/delete_{uuid.uuid1()}"
            open(f'{file_name}.json', 'wb').write(delete.json())
        self.session.close()


    def set_holding(self, oclc_number, symbols):

        divided_symbols = list(self.divide_chunks(symbols, 50))

        for library_symbols in divided_symbols:
            url_symbols = ','.join(library_symbols)
            url = f"https://worldcat.org/ih/institutionlist?instSymbols={quote(url_symbols,safe='/,')}&oclcNumber={oclc_number}"
            add = self.session.post(url=url, headers=self.headers)
            file_name = f"results/add_{uuid.uuid1()}"
            open(f'{file_name}.json', 'wb').write(add.json())
        self.session.close()

    def __init__(self, oclc_credentials: tuple):
        self.client_id = oclc_credentials[0]
        self.client_secret = oclc_credentials[1]
        self.headers = {'Accept': 'application/json'}
        self.token = None
        self.session = None

        self.oclc_login()