from CyberSource import *
from pathlib import Path
import os
import json
from importlib.machinery import SourceFileLoader

config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

# To delete None values in Input Request Json body
def del_none(d):
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d

def enroll_instrument_identifier_for_network_tokenization():
    profileid = "93B32398-AD51-4CC2-A682-EA3E93614EB1"
    instrumentIdentifierTokenId = "7010000000016241111"

    type = "enrollable card"
    cardExpirationMonth = "12"
    cardExpirationYear = "2031"
    cardSecurityCode = "123"
    card = TmsEmbeddedInstrumentIdentifierCard(
        expiration_month = cardExpirationMonth,
        expiration_year = cardExpirationYear,
        security_code = cardSecurityCode
    )

    billToAddress1 = "1 Market St"
    billToLocality = "San Francisco"
    billToAdministrativeArea = "CA"
    billToPostalCode = "94105"
    billToCountry = "US"
    billTo = TmsEmbeddedInstrumentIdentifierBillTo(
        address1 = billToAddress1,
        locality = billToLocality,
        administrative_area = billToAdministrativeArea,
        postal_code = billToPostalCode,
        country = billToCountry
    )

    requestObj = PostInstrumentIdentifierEnrollmentRequest(
        type = type,
        card = card.__dict__,
        bill_to = billTo.__dict__
    )


    requestObj = del_none(requestObj.__dict__)
    requestObj = json.dumps(requestObj)

    try:
        config_obj = configuration.Configuration()
        client_config = config_obj.get_configuration()
        api_instance = InstrumentIdentifierApi(client_config)
        return_data, status, body = api_instance.post_instrument_identifier_enrollment(instrumentIdentifierTokenId, requestObj, profile_id=profileid)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)
        return return_data
    except Exception as e:
        write_log_audit(e.status if hasattr(e, 'status') else 999)
        print("\nException when calling InstrumentIdentifierApi->post_instrument_identifier_enrollment: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    enroll_instrument_identifier_for_network_tokenization()
