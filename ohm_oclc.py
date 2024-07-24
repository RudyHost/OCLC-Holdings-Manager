import requests
import json
import os
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import time
from urllib.parse import quote
import uuid
import glob
import gc

class OhmOclc:

    def token_updater(self, new_token):
        self.token = new_token
        return None

    def oclc_login(self, institution_id):
        scopes = ['WorldCatMetadataAPI', 'refresh_token', f'context:{institution_id}']
        token_url = 'https://oauth.oclc.org/token'
        extra = {
            'client_id': self.client_id,
        }

        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        client = BackendApplicationClient(client_id=self.client_id, scopes=scopes, auto_refresh_url=token_url, auto_refresh_kwargs=extra, token_updater=self.token_updater)

        try:
            self.session = OAuth2Session(client=client,)
            self.session.fetch_token(token_url=token_url, auth=auth, scope=scopes, include_client_id=True, )
        except:
            self.retry += 1
            sleep_time = 10 * self.retry
            print(f"Failed OCLC API login, retrying in {sleep_time} seconds.")
            self.session.close()
            time.sleep(sleep_time)
            self.oclc_login(institution_id)
        
        self.retry = 0

    def test_wskey(self, holding_map):

        #if self.session == None:
        #    self.oclc_login()

        symbols = set()
        for i, (ils, oclc) in enumerate(holding_map.items()):
            symbols.add(oclc)

        failed_symbols = dict()

        for symbol in symbols:
            print(f"Setting hold for {symbol}")
            self.oclc_login(symbol)
            add_url = "https://metadata.api.oclc.org/worldcat/manage/institution/holdings/953197097/set"
            add = self.session.post(url=add_url, headers=self.headers)
            response = json.loads(add.text)
            try:
                if not add.ok:
                    failed_symbols[symbol] = response['message']
            finally:
                add.close()
            self.session.close()
            if response in locals():
                del response
            if add in locals():
                del add
            gc.collect()

        time.sleep(10)

        for symbol in symbols:
            print(f"Unsetting hold for {symbol}")
            self.oclc_login(symbol)
            delete_url = "https://metadata.api.oclc.org/worldcat/manage/institution/holdings/953197097/unset"
            delete = self.session.post(url=delete_url, headers=self.headers)
            response = json.loads(delete.text)
            try: 
                if not delete.ok:
                    failed_symbols[symbol] = response['message']
                else:
                    response = json.loads(delete.text)
                    if not response["success"]:
                        failed_symbols[symbol] = response['message']
            finally:
                delete.close()
            self.session.close()
            if response in locals():
                del response
            if delete in locals():
                del delete
            gc.collect()
        
        self.session.close()
        self.session = None
        gc.collect()
        
        return failed_symbols
    
    def search_lbd(self, oclc_number, institution_id):
        if self.session == None:
            self.oclc_login(institution_id)

        if self.session.token['context_institution_id'] != institution_id:
            self.oclc_login(institution_id)

        url = f"https://metadata.api.oclc.org/worldcat/search/my-local-bib-data?q=oc:{oclc_number}"
        lbd_control = []
        
        try:
            search = self.session.get(url=url, headers=self.headers)
            print(url)

            if not search.ok:
                print("API call failed.")
            else:
                response = json.loads(search.text)
                if response['numberOfRecords'] == 0:
                    return lbd_control
                for record in response['localBibData']:
                    lbd_control.append(record['controlNumber'])
                del response
        except:
            self.retry += 1
            sleep_time = 10 * self.retry
            print(f"Failed operation on {oclc_number}, retrying in {sleep_time} seconds.")
            self.session = None
            time.sleep(sleep_time)
            self.oclc_login(institution_id)
            lbd_control = self.search_lbd(oclc_number, institution_id)
        
        finally:
            del search
            self.session.close()
            self.retry = 0
            gc.collect()
            return lbd_control

    def delete_lbd(self, lbd_control, institution_id):
        if self.session == None:
            self.oclc_login(institution_id)

        if self.session.token['context_institution_id'] != institution_id:
            self.oclc_login(institution_id)

        url = f"https://metadata.api.oclc.org/worldcat/manage/lbds/{lbd_control}"
        
        try:
            headers = {'Accept': 'application/marcxml+xml'}
            delete = self.session.delete(url=url, headers=headers)
            print(url)

            #response = json.loads(delete.text)
            if not delete.ok:
                print("API call failed.")
            else:
                pass # Add proper handling of records
        except:
            self.retry += 1
            sleep_time = 10 * self.retry
            print(f"Failed operation on {lbd_control}, retrying in {sleep_time} seconds.")
            self.session.close()
            time.sleep(sleep_time)
            self.oclc_login(institution_id)
            self.delete_lbd(lbd_control, institution_id)
        
        self.session.close()
        self.retry = 0


    def unset_holding(self, oclc_number, institution_id, results_directory = "results"):

        if self.session == None:
            self.oclc_login(institution_id)

        if self.session.token['context_institution_id'] != institution_id:
            self.oclc_login(institution_id)

        url = f"https://metadata.api.oclc.org/worldcat/manage/institution/holdings/{oclc_number}/unset"
        
        try:
            delete = self.session.post(url=url, headers=self.headers)
            print(url)
            file_name = f"{results_directory}/delete_{uuid.uuid1()}"
            with open(f'{file_name}.json', 'w') as results_file:
                results_file.write(delete.text)
                results_file.flush()
                results_file.close()
            del file_name

            response = json.loads(delete.text)
            if not delete.ok:
                print("API call failed.")
            else:
                response = json.loads(delete.text)
                if not response["success"]:
                    print(response['message'])
                    if "LBD" in response['message']:
                        lbd_records = self.search_lbd(oclc_number, institution_id)
                        if lbd_records:
                            for lbd in lbd_records:
                                self.delete_lbd(lbd, institution_id)
                            self.unset_holding(oclc_number, institution_id)
                        del lbd_records
                del response
        except:
            self.retry += 1
            sleep_time = 10 * self.retry
            print(f"Failed operation on {oclc_number}, retrying in {sleep_time} seconds.")
            self.session.close()
            self.session = None
            time.sleep(sleep_time)
            self.oclc_login(institution_id)
            self.unset_holding(oclc_number, institution_id)
        
        finally:
            del delete
            gc.collect()
        
        self.session.close()
        self.retry = 0


    def set_holding(self, oclc_number, institution_id, results_directory = "results"):

        if self.session == None:
            self.oclc_login(institution_id)

        if self.session.token['context_institution_id'] != institution_id:
            self.oclc_login(institution_id)

        url = f"https://metadata.api.oclc.org/worldcat/manage/institution/holdings/{oclc_number}/set"

        try:
            add = self.session.post(url=url, headers=self.headers)
            print(url)
            file_name = f"{results_directory}/add_{uuid.uuid1()}"
            with open(f'{file_name}.json', 'w') as results_file:
                results_file.write(add.text)
                results_file.flush()
                results_file.close()
            del file_name

        except:
            self.retry += 1
            sleep_time = 10 * self.retry
            print(f"Failed operation on {oclc_number}, retrying in {sleep_time} seconds.")
            self.session = None
            time.sleep(sleep_time)
            self.oclc_login(institution_id)
            self.set_holding(oclc_number, institution_id)

        finally:
            del add
            gc.collect()

        self.session.close()
        self.retry = 0
    
    def count_results(self, input):
        count = 0
        for library in input.keys():
            number = len(input[library])
            count += number
            print(f"{library}: {number}")
        return count

    def analyze_files(self, file_list):
        successful_action = dict()
        unsuccessful_action = dict()
        updated_oclc = dict()

        for file_name in file_list:
            with open(file_name) as result_log:
                result = json.load(result_log)
                
                oclc_symbol = result['institutionSymbol']
                oclc_number = result['requestedControlNumber']
                current_oclc_number = result['controlNumber']
                success = result['success']
                
                if success:
                    if oclc_symbol not in successful_action:
                        successful_action[oclc_symbol] = list()
                    successful_action[oclc_symbol].append(oclc_number)
                else:
                    if oclc_symbol not in unsuccessful_action:
                        unsuccessful_action[oclc_symbol] = list()
                    unsuccessful_action[oclc_symbol].append(oclc_number)
                if oclc_number != current_oclc_number:
                    updated_oclc[oclc_number] = current_oclc_number
        
        return (successful_action, unsuccessful_action, updated_oclc)

    def analyze_results(self, results_directory = "results", print_stats = True, file_prefix = ""):
        if file_prefix:
            prefix = f"{file_prefix}_"
        else:
            prefix = ""

        set_files = glob.glob(f'{results_directory}/add_*.json')
        unset_files = glob.glob(f'{results_directory}/delete_*.json')
        
        analyzed_sets = self.analyze_files(set_files)
        analyzed_unsets = self.analyze_files(unset_files)
        
        successful_sets = analyzed_sets[0]
        unsuccessful_sets = analyzed_sets[1]
        successful_unsets = analyzed_unsets[0]
        unsuccessful_unsets = analyzed_unsets[1]
        # Merge the updated OCLC numbers from both actions
        updated_oclc = {**analyzed_unsets[2], **analyzed_unsets[2]}

        if print_stats:
            print("Sets by library:")
            print(f'Total: {self.count_results(successful_sets)}\n')

            print("Unchanged sets by library:")
            print(f'Total: {self.count_results(unsuccessful_sets)}\n')

            print("Unsets by library:")
            print(f'Total: {self.count_results(successful_unsets)}\n')

            print("Unchanged unsets by library:")
            print(f'Total: {self.count_results(unsuccessful_unsets)}\n')

            print(f"Updated OCLC numbers: {len(updated_oclc)}")

        with open(f'{prefix}successful_sets.json', 'w') as successful_sets_file:
            json.dump(successful_sets, successful_sets_file, indent=4)

        with open(f'{prefix}unsuccessful_sets.json', 'w') as unsuccessful_sets_file:
            json.dump(unsuccessful_sets, unsuccessful_sets_file, indent=4)

        with open(f'{prefix}successful_unsets.json', 'w') as successful_unsets_file:
            json.dump(successful_unsets, successful_unsets_file, indent=4)

        with open(f'{prefix}unsuccessful_unsets.json', 'w') as unsuccessful_unsets_file:
            json.dump(unsuccessful_unsets, unsuccessful_unsets_file, indent=4)

        with open(f'{prefix}updated_oclc.json', 'w') as updated_oclc_file:
            json.dump(updated_oclc, updated_oclc_file, indent=4)

    def __init__(self, oclc_credentials: tuple):
        self.client_id = oclc_credentials[0]
        self.client_secret = oclc_credentials[1]
        self.headers = {'Accept': 'application/json'}
        self.token = None
        self.session = None
        self.retry = 0