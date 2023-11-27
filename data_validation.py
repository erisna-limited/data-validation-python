"""
Short description of this Python module.
This program is free demo/example software by Erisna Limited to run data 
validation checks using business rules and dataset schemas in 
Erisna API against actual data.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = "Erisna Limited"
__authors__ = ["Erisna Limited"]
__contact__ = "support@erisna.com"
__copyright__ = "Copyright 2021 - 2023, Erisna Limited"
__credits__ = ["Erisna Limited"]
__date__ = "2023/11/27"
__deprecated__ = False
__email__ =  "support@erisna.com"
__license__ = "GPLv3"
__maintainer__ = "Erisna Limited"
__status__ = "Production"
__version__ = "0.0.1"


import os
import json
import requests
import pandas as pd


erisna_api_url_endpoints = {
    "project"                : "https://api.erisna.com/project/",
    "business_term"          : "https://api.erisna.com/business-term/",
    "data_source"            : "https://api.erisna.com/data-source/",
    "dataset"                : "https://api.erisna.com/dataset/",
    "dataset_extra_metadata" : "https://api.erisna.com/extra-metadata/",
    "field"                  : "https://api.erisna.com/field/",
    "update_field"           : "https://api.erisna.com/update-field/"
}


validation_ids = {
    "project_id"                      : int("<PROJECT-ID>"),
    "data_source_id"                  : int("<DATA-SOURCE-ID>"),
    "datasets_id"                     : int("<EXTRA-METADATA-ID>"),
    "valid_credit_card_number_regex"  : int("<EXTRA-METADATA-ID>"),
    "valid_credit_card_number_digits" : int("<EXTRA-METADATA-ID>"),
    "valid_date_regex"                : int("<EXTRA-METADATA-ID>"),
    "valid_date_format"               : int("<EXTRA-METADATA-ID>"),
    "max_expected_records"            : int("<EXTRA-METADATA-ID>"),
    "min_expected_records"            : int("<EXTRA-METADATA-ID>")

}


def get_erisna_api_connection_string():
    """
    Get Erisna API connection string from system environment variable

    Returns
    -------
    headers : dict
    """
    api_key = "<YOUR-ORG-GENERATED-API-KEY>"
    headers = {
        'X-Api-Key': f'{api_key}'
    }
    return headers


def read_transaction_data():
    """
    Read data from CSV into Pandas DataFrame

    Returns
    -------
    df : pandas.core.frame.DataFrame
    """
    df = pd.read_csv('Transactions.csv')
    return df


def read_extra_metadata_from_erisna(headers):
    """
    Send a HTTP GET request using Erisna API to get metadata payload from Erisna that will be used for data validation

    Parameters
    ----------
    headers : dict

    Returns
    -------
    response.text : str
    """
    payload = {}
    response = requests.request("GET", erisna_api_url_endpoints["dataset_extra_metadata"], headers=headers, data=payload)
    return response.text


def validate_min_expected_records(transactions_df, extra_metadata_payload_str):
    """
    Validate that the number of records is within the minimum expected threshold

    Parameters
    ----------
    transactions_df : pandas.core.frame.DataFrame
    extra_metadata_payload_str : str

    Returns
    -------
    is_valid : bool
    """
    extra_metadata_payload = json.loads(extra_metadata_payload_str)
    extra_metadata_payload = extra_metadata_payload["erisna_api_dataset_extra_metadata_list"]
    min_expected_records = None
    is_valid = False

    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            print("\nMINIMUM_EXPECTED_RECORDS:")
            print(item)
            min_expected_records = item["attributes"]["extra_metadata_value"]
            actual_record_count = transactions_df.shape[0]

            print(f"[max_expected_records: {min_expected_records}]")
            print(f"[actual_record_count: {actual_record_count}]")

            if actual_record_count >= int(min_expected_records):
                is_valid = True
                break
    return is_valid


def validate_max_expected_records(transactions_df, extra_metadata_payload_str):
    """
    Validate that the number of records is within the maximum expected threshold

    Parameters
    ----------
    transactions_df : pandas.core.frame.DataFrame
    extra_metadata_payload_str : str

    Returns
    -------
    is_valid : bool
    """
    extra_metadata_payload = json.loads(extra_metadata_payload_str)
    extra_metadata_payload = extra_metadata_payload["erisna_api_dataset_extra_metadata_list"]
    max_expected_records = None
    is_valid = False

    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            print("\nMAXIMUM_EXPECTED_RECORDS:")
            print(item)
            max_expected_records = item["attributes"]["extra_metadata_value"]
            actual_record_count = transactions_df.shape[0]

            print(f"[max_expected_records: {max_expected_records}]")
            print(f"[actual_record_count: {actual_record_count}]")

            if actual_record_count <= int(max_expected_records):
                is_valid = True
                break
    return is_valid


def validate_date_format(transactions_df, extra_metadata_payload_str):
    """
    Validate that the date column follows the DD-MM-YYYY date format.

    Parameters
    ----------
    transactions_df : pandas.core.frame.DataFrame
    extra_metadata_payload_str : str

    Returns
    -------
    is_valid : bool
    """
    extra_metadata_payload = json.loads(extra_metadata_payload_str)
    extra_metadata_payload = extra_metadata_payload["erisna_api_dataset_extra_metadata_list"]
    valid_date_format = None
    valid_date_regex_pattern = None
    is_valid = False
    
    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            valid_date_format = item["attributes"]["extra_metadata_value"]
    
    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            print("\nDATE_FORMAT:")
            print(item)
            valid_date_regex_pattern = item["attributes"]["extra_metadata_value"]
            all_dates_match_pattern = transactions_df['Date'].str.match(valid_date_regex_pattern).all()

            if all_dates_match_pattern:
                print(f"[expected_date_format: {valid_date_format}]")
                print(f"[actual_date_format: {valid_date_format}]")

                is_valid = True
                break
            else:
                print(f"[expected_date_format: {valid_date_format}]")
                print(f"[actual_date_format: INVALID DATE FORMAT]")
                break
    return is_valid


def validate_credit_card_number_format(transactions_df, extra_metadata_payload_str, headers):
    """
    Validate that the credit card number column follows the 16-digits format.

    Parameters
    ----------
    transactions_df : pandas.core.frame.DataFrame
    extra_metadata_payload_str : str

    Returns
    -------
    is_valid : bool
    """
    extra_metadata_payload = json.loads(extra_metadata_payload_str)
    extra_metadata_payload = extra_metadata_payload["erisna_api_dataset_extra_metadata_list"]
    valid_credit_card_number_format = None
    valid_credit_card_number_regex_pattern = None
    is_valid = False
    
    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            valid_credit_card_number_format = item["attributes"]["extra_metadata_value"]
    
    for item in extra_metadata_payload:
        if item["id"] == int("<EXTRA-METADATA-ID>"):
            print("\nCREDIT_CARD_NUMBER_FORMAT:")
            print(item)
            valid_credit_card_number_regex_pattern = item["attributes"]["extra_metadata_value"]
            
            transactions_df["Credit Card Number"] = transactions_df["Credit Card Number"].astype(str)
            transactions_df["Credit Card Number"] = transactions_df["Credit Card Number"].str.replace(' ', '')
            all_credit_card_numbers_match_pattern = transactions_df['Credit Card Number'].str.match(valid_credit_card_number_regex_pattern).all()


            if all_credit_card_numbers_match_pattern:
                print(f"[expected_credit_card_number_format: {valid_credit_card_number_format}-digits]")
                print(f"[actual_credit_card_number_format: {valid_credit_card_number_format}-digits]")

                is_valid = True
                break
            else:
                api_key = os.environ.get('ERISNA_API_KEY')
                request_headers = {
                    'X-Api-Key': f'{api_key}',
                    'Content-Type': 'application/json'
                }

                # Send HTTP PATCH (update) Request via Erisna API 
                expected_msg = f"[expected_credit_card_number_format: {valid_credit_card_number_format}-digits]"
                actual_msg = f"[actual_credit_card_number_format: INVALID CREDIT CARD NUMBER FORMAT]"

                print(expected_msg)
                print(actual_msg)

                payload = json.dumps({
                    "dataset_field_flag": "Yes",
                    "dataset_field_flag_note": f"ERROR: Data validation failed. INVALID CREDIT CARD NUMBER FORMAT. Expected format {valid_credit_card_number_format}-digits."
                })

                field_id = int("<FIELD-ID>")
                url = f"""{erisna_api_url_endpoints["update_field"]}{field_id}/"""

                response = requests.request("PATCH", url, headers=request_headers, data=payload)
                print(f"SENT HTTP PATCH Request to Erisna. \n{response}")

                break
    return is_valid


def run():
    """
    Run data validation checks using business rules and dataset schemas in Erisna API against actual data.
    """
    headers = get_erisna_api_connection_string()

    transactions_df = read_transaction_data()
    print(transactions_df)

    extra_metadata_payload = read_extra_metadata_from_erisna(headers=headers)
    # print(extra_metadata_payload)

    is_valid_min_expected_records = validate_min_expected_records(transactions_df, extra_metadata_payload)
    print("is_valid_min_expected_records >>", is_valid_min_expected_records)

    is_valid_max_expected_records = validate_max_expected_records(transactions_df, extra_metadata_payload)
    print("is_valid_max_expected_records >>", is_valid_max_expected_records)

    is_valid_date_format =  validate_date_format(transactions_df, extra_metadata_payload)
    print("is_valid_date_format >>", is_valid_date_format)

    is_valid_credit_card_number_format =  validate_credit_card_number_format(transactions_df, extra_metadata_payload, headers)
    print("is_valid_credit_card_number_format >>", is_valid_credit_card_number_format)


if __name__ == '__main__':
    run()
