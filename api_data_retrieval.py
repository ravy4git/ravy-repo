import configparser
import csv
import json
import logging
import os.path
import re
import requests
import sys
import time
import traceback
import base64
import getopt
import sys

# Variables for retry logic in the case of failure while pulling data
retry_limit = 3
retry_attempts = 0
sleep_time = 10

# Variables for Logging
logging.basicConfig()
LOGGER = logging.getLogger("Ravy Public API")
LOGGER.setLevel(logging.DEBUG)

#other variables
all_lob = "all"

#reading command line input
options, remainder = getopt.getopt(sys.argv[1:], ':', ['configpath=',
                                                         'responseformat=',
                                                         'filename=',
                                                         'targetpath=',
                                                         'authurl=',
                                                         'apiurl=',
                                                         'clientid=',
                                                         'clientsecret=',
                                                         'startdate=',
                                                         'enddate=',
                                                         'locale=',
                                                         'currencycode=',
                                                         'lob=',
                                                         'reportlevel=',
                                                         'includes=',
                                                         'datetype=',
                                                         'activerecordsonly='
                                                         ])

for opt, arg in options:
        if opt in ['--configpath']:
            config_path = arg
        if opt in ['--responseformat']:
            response_format = arg
        if opt in ['--filename']:
            default_file_name = arg
        if opt in ['--targetpath']:
            target_path = arg
        if opt in ['--authurl']:
            auth_url = arg
        if opt in ['--apiurl']:
            api_url = arg
        if opt in ['--clientid']:
            client_id = arg
        if opt in ['--clientsecret']:
            client_secret = arg
        if opt in ['--startdate']:
            start_date = arg
        if opt in ['--enddate']:
            end_date = arg
        if opt in ['--locale']:
            locale = arg
        if opt in ['--currencycode']:
            currency_code = arg
        if opt in ['--lob']:
            line_of_business = arg
        if opt in ['--reportlevel']:
            report_level = arg
        if opt in ['--includes']:
            includes = arg.split(",")
        if opt in ['--datetype']:
            date_type = arg
        if opt in ['--activerecordsonly']:
            active_records_only = arg

#reading from config file as a fallback if not provided as command line input
if 'config_path' in globals():
    config = configparser.ConfigParser()
    configfile = open(config_path)
    config.read_file(configfile)
    if 'response_format' not in globals(): response_format = config.get('DEFAULT', "responseformat")
    if 'default_file_name' not in globals(): default_file_name = config.get('DEFAULT', "filename")
    if 'target_path' not in globals(): target_path = config.get('DEFAULT', "targetpath")
    if 'auth_url' not in globals(): auth_url = config.get('DEFAULT', "authurl")
    if 'api_url' not in globals(): api_url = config.get('DEFAULT', "apiurl")
    if 'client_id' not in globals(): client_id = config.get('CREDENTIALS', "clientid")
    if 'client_secret' not in globals(): client_secret = config.get('CREDENTIALS', "clientsecret")
    if 'start_date' not in globals(): start_date = config.get('FILTERS', "startdate")
    if 'end_date' not in globals(): end_date = config.get('FILTERS', "enddate")
    if 'line_of_business' not in globals(): line_of_business = config.get('FILTERS', "lob")
    if 'active_records_only' not in globals():
        try:
            active_records_only = config.get('FILTERS',"activerecordsonly")
        except configparser.NoOptionError:
            pass
    if 'report_level' not in globals():
        try:
            report_level = config.get('FILTERS', 'reportlevel')
        except configparser.NoOptionError:
            pass
    if 'includes' not in globals():
        try:
            includes = config.get('FILTERS', 'includes').split(",")
        except configparser.NoOptionError:
            pass
    if 'currency_code' not in globals():
        try:
            currency_code = config.get('FILTERS', 'currencycode')
        except configparser.NoOptionError:
            pass
    if 'locale' not in globals():
        try:
            locale = config.get('FILTERS', 'locale')
        except configparser.NoOptionError:
            pass
    if 'date_type' not in globals():
        try:
            date_type = config.get('FILTERS','datetype')
        except configparser.NoOptionError:
            pass
    configfile.close()

def fetch_auth_token(auth_url, basic_auth, client_id):
    try:
        data = {"grant_type": "client_credentials", "client_id": client_id}
        response = requests.post(auth_url, data=data, headers={'authorization': basic_auth, 'Content-Type': 'application/x-www-form-urlencoded'})
        if (response.ok):
            response_data = response.json()
            return 'Bearer ' + response_data["access_token"]
        else:
            response.raise_for_status()
            LOGGER.error(response)
    except Exception as ex:
        LOGGER.error("Error invoking the authentication endpoint: %s" % traceback.format_exc())
        raise ex

def invoke_post_endpoint(url, data, auth, lob):
    try:
        data = json.dumps(data)
        if(lob != all_lob):
            url = url + "/" + lob
        api_response = requests.post(url, data=data, headers={'authorization': auth, 'Content-Type': 'application/json', 'accept': 'application/hal+json'})
        if api_response.status_code == 204:
            return {}
        elif api_response.ok:
            api_data = api_response.json()
            LOGGER.info("Report ID is: " + api_data["report_id"])
            LOGGER.info("Total number of records are: %s", api_data['metadata']['total_records'])
            LOGGER.info("Page limit is: %s", api_data['metadata']['page_limit'])
            total_pages = api_data['metadata']['total_pages']
            LOGGER.info("Total pages are: %s", total_pages)
            return api_data
        else:
            api_response.raise_for_status()
            LOGGER.error(api_response)
    except Exception as ex:
        LOGGER.error("Error invoking the POST API: %s" % traceback.format_exc())
        raise ex

def invoke_get_endpoint(url, auth):
    try:
        api_response = requests.get(url, headers={'authorization': auth, 'Content-Type': 'application/json', 'accept': 'application/hal+json'})
        if (api_response.ok):
            api_data = api_response.json()
            total_pages = api_data['metadata']['total_pages']
            current_page = api_data['metadata']['current_page']
            LOGGER.info("Accessing page: %s", current_page)
            return api_data
        else:
            api_response.raise_for_status()
            LOGGER.error(api_response)
    except Exception as ex:
        LOGGER.error("Error invoking the GET API: %s" % traceback.format_exc())
        raise ex

# Retry Logic
# In case of failure to reach API, wait 10 seconds and try again until the retry limit is hit
def retry(ex, retry_limit, sleep_time):
    global retry_attempts
    LOGGER.error(ex)
    if (retry_attempts < retry_limit):
        retry_attempts += 1
        LOGGER.error("Retry attempt #{}".format(retry_attempts))
        LOGGER.error("Waiting {} seconds before retrying...".format(sleep_time))
        time.sleep(sleep_time)
    else:
        LOGGER.error("Failed connection {} times, exiting...".format(retry_limit))
        sys.exit()

# Write result to a target path as a json file
def write_to_json_file(json_data):
    with open(target_path + ".json", 'a+', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, ensure_ascii=False, indent=2)

# Write result to a target path as a csv file
def write_to_csv_file(result):
    data = []
    for entries in result:
        for entry in entries:
            flat_json = flatten_json(entry)
            data.append(flat_json)
    fieldnames = set()
    for entry in data:
        # First parse all entries to get the complete fieldname list
        fieldnames.update(get_leaves(entry).keys())
    with open(target_path + ".csv", "w", newline="", encoding='utf-8') as f_output:
        csv_output = csv.DictWriter(f_output, fieldnames=sorted(fieldnames))
        csv_output.writeheader()
        csv_output.writerows(get_leaves(entry) for entry in data)

#flatten the json to handle nesting
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

#find all leaf nodes in json to get all unique columns for CSV
def get_leaves(item, key=None, key_prefix=""):
    #This function converts nested dictionary structure to flat
    if isinstance(item, dict):
        leaves = {}
        #Iterates the dictionary and go to leaf node after that calls to get_leaves function recursively to go to leaves level
        for item_key in item.keys():
            #Some times leaves and parents or some other leaves might have same key that's why adding leave node key to distinguish
            temp_key_prefix = (
                item_key if (key_prefix == "") else (key_prefix + "_" + str(item_key))
            )
            leaves.update(get_leaves(item[item_key], item_key, temp_key_prefix))
        return leaves
    elif isinstance(item, list):
        leaves = {}
        elements = []
        """Iterates the list and go to leaf node after that if it is leave then simply add value to current key's list or
        calls to get_leaves function recursively to go to leaves level"""
        for element in item:
            if isinstance(element, dict) or isinstance(element, list):
                leaves.update(get_leaves(element, key, key_prefix))
            else:
                elements.append(element)
        if len(elements) > 0:
            leaves[key] = elements
        return leaves
    else:
        return {key_prefix: item}

def generate_basic_auth(client_id, client_secret):
    try:
        message = client_id + ":" + client_secret
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        basic_auth = 'Basic ' + base64_message
        return basic_auth
    except Exception as ex:
        LOGGER.error("Oops! Exception occured while encoding the credentials")
        raise ex

if __name__ == '__main__':
    try:
        # Prepare request data
        # Header data for the API call
        data = {"start_date": start_date, "end_date": end_date}
        if 'includes' in globals():
            data.update({"includes": includes})
        if 'report_level' in globals():
            data.update({"report_level": report_level})
        if 'currency_code' in globals():
            data.update({"currency_code": currency_code})
        if 'locale' in globals():
            data.update({"locale": locale})
        if 'date_type' in globals():
            data.update({"date_type": date_type})
        if 'active_records_only' in globals():
            data.update({"active_records_only":active_records_only})
        # if the target download path leads to a directory, append a file name to it
        if target_path[-1] == '\\' or target_path[-1] == "/":
            # Check that the target path is valid
            if not os.path.exists(target_path):
                err_message = "Oops! The directory \"{} doesn't exist!\"".format(target_path)
                raise OSError(err_message)
            target_path += str(default_file_name) + time.strftime("%Y_%m_%d-%H_%M_%S")
        else:
            try:
                # used to test if file name is valid
                f = open(target_path, "a+")
                f.close()
                # if the file is new (size == 0), it means we just created it, so it is safe to delete
                if (os.stat(target_path).st_size == 0):
                    os.remove(target_path)
            except FileNotFoundError:
                err_message = "Oops! \"{}\" is an invalid file name!".format(target_path)
                raise FileNotFoundError(err_message)
    except IndexError as ex:
        LOGGER.error("Oops! No config file path entered!")
        raise ex
    except Exception as ex:
        LOGGER.error("Oops! Exception occured")
        raise ex

    # If the response format is not json or csv, it is not supported
    if (response_format.lower() != 'json' and response_format.lower() != 'csv'):
        LOGGER.error("Invalid download format, defaulting to json")
        response_format = 'json'

    result = []
    basic_auth = generate_basic_auth(client_id, client_secret)
    transactions_found = False
    next_page_exists = True

    while retry_attempts <= retry_limit and transactions_found == False and next_page_exists == True:
        # Retrieve the next or last page whichever is applicable
        try:
            auth_token = fetch_auth_token(auth_url, basic_auth, client_id)
            api_post_data = invoke_post_endpoint(api_url, data, auth_token, line_of_business)
            if len(api_post_data) == 0:
                next_page_exists = False
                break
            total_pages = api_post_data['metadata']['total_pages']
            if (total_pages > 1):
                get_endpoint_url = api_post_data["_links"]['next']['href']
                api_get_data = invoke_get_endpoint(get_endpoint_url, auth_token)
            else:
                get_endpoint_url = api_post_data["_links"]['last']['href']
                api_get_data = invoke_get_endpoint(get_endpoint_url, auth_token)
            transactions_found = True
        except requests.HTTPError as ex:
            LOGGER.error(ex.response.content.decode('utf-8'))
            if (ex.response.status_code < 500 and ex.response.status_code >= 400):
                next_page_exists = False
            else:
                retry(ex, retry_limit, sleep_time)
        except Exception as ex:
            retry(ex, retry_limit, sleep_time)

    retry_attempts = 0
    while next_page_exists:
        try:
            auth_token = fetch_auth_token(auth_url, basic_auth, client_id)
            result.append(api_get_data["transactions"])
            # While there are more pages to be retrieved, retrieve the next page
            total_pages = api_get_data['metadata']['total_pages']
            current_page = api_get_data['metadata']['current_page']
            if(total_pages == current_page):
                next_page_exists = False
            elif(total_pages > current_page):
                get_endpoint_url = api_get_data["_links"]['next']['href']
                api_get_data = invoke_get_endpoint(get_endpoint_url, auth_token)
            else:
                get_endpoint_url = api_get_data["_links"]['last']['href']
                api_get_data = invoke_get_endpoint(get_endpoint_url, auth_token)
                next_page_exists = False
        except requests.HTTPError as ex:
            LOGGER.error(ex.response.content.decode('utf-8'))
            if (ex.response.status_code < 500 and ex.response.status_code >= 400):
                next_page_exists = False
            else:
                retry(ex, retry_limit, sleep_time)
        except Exception as ex:
            retry(ex, retry_limit, sleep_time)

    # If there are any results, export them to either json or csv
    # Else, do not export anything and log that no results were found
    if len(result) > 0 and len(result[0]) > 0:
        if (response_format.lower() == 'json'):
            write_to_json_file(result)
        elif (response_format.lower() == 'csv'):
            write_to_csv_file(result)
        LOGGER.info("Successfully retrieved data")
    else:
        LOGGER.info("No results found")
