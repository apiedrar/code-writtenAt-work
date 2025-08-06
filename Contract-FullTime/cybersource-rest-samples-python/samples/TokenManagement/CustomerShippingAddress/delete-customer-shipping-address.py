from CyberSource import *
from pathlib import Path
import os
import json
from importlib.machinery import SourceFileLoader

create_customer_shipping_address_path = os.path.join(os.getcwd(), "samples", "TokenManagement", "CustomerShippingAddress", "create-customer-nondefault-shipping-address.py")
create_customer_shipping_address = SourceFileLoader("module.name", create_customer_shipping_address_path).load_module()

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

def delete_customer_shipping_address():
    customerTokenId = "AB695DA801DD1BB6E05341588E0A3BDC"
   
    try:
        api_response = create_customer_shipping_address.create_customer_nondefault_shipping_address()
        shippingAddressTokenId = api_response.id
        config_obj = configuration.Configuration()
        client_config = config_obj.get_configuration()
        api_instance = CustomerShippingAddressApi(client_config)
        return_data, status, body = api_instance.delete_customer_shipping_address(customerTokenId, shippingAddressTokenId)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)
        return return_data
    except Exception as e:
        write_log_audit(e.status if hasattr(e, 'status') else 999)
        print("\nException when calling CustomerShippingAddressApi->delete_customer_shipping_address: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    delete_customer_shipping_address()
