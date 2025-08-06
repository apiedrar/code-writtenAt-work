from CyberSource import *
import os
import json
from importlib.machinery import SourceFileLoader
from pathlib import Path

config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

create_subscription_path = os.path.join(os.getcwd(), "samples", "RecurringBillingSubscriptions", "Subscriptions", "create-subscription.py")
create_subscription = SourceFileLoader("module.name", create_subscription_path).load_module()

# To delete None values in Input Request Json body
def del_none(d):
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d

def get_subscription():

    try:
        # create_subscription_response = create_subscription.create_subscription()
        # The following `id` field is hardcoded because the above call will not allow duplicate requests.
        id = "6971805775636334604953" # create_subscription_response.id
        config_obj = configuration.Configuration()
        client_config = config_obj.get_configuration()
        api_instance = SubscriptionsApi(client_config)
        return_data, status, body = api_instance.get_subscription(id)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)

        return return_data
    except Exception as e:
        write_log_audit(e.status)
        print("\nException when calling SubscriptionsApi->get_subscription: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    get_subscription()