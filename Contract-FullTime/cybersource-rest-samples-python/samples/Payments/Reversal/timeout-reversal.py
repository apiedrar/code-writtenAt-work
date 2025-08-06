from CyberSource import *
from pathlib import Path
import os
import json
from importlib.machinery import SourceFileLoader

config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

authorization_path = os.path.join(os.getcwd(), "samples", "Payments", "Payments", "authorization-for-timeout-reversal-flow.py")
authorization = SourceFileLoader("module.name", authorization_path).load_module()

# To delete None values in Input Request Json body
def del_none(d):
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d

def timeout_reversal():
    # id = authorization.authorization_for_timeout_reversal_flow().id
    timeoutReversalTransactionId = authorization.timeoutReversalTransactionId

    clientReferenceInformationCode = "TC50171_3"
    clientReferenceInformationTransactionId = timeoutReversalTransactionId
    clientReferenceInformation = Ptsv2paymentsClientReferenceInformation(
        code = clientReferenceInformationCode,
        transaction_id = clientReferenceInformationTransactionId
    )

    reversalInformationAmountDetailsTotalAmount = "102.21"
    reversalInformationAmountDetails = Ptsv2paymentsidreversalsReversalInformationAmountDetails(
        total_amount = reversalInformationAmountDetailsTotalAmount
    )

    reversalInformationReason = "testing"
    reversalInformation = Ptsv2paymentsidreversalsReversalInformation(
        amount_details = reversalInformationAmountDetails.__dict__,
        reason = reversalInformationReason
    )

    requestObj = MitReversalRequest(
        client_reference_information = clientReferenceInformation.__dict__,
        reversal_information = reversalInformation.__dict__
    )


    requestObj = del_none(requestObj.__dict__)
    requestObj = json.dumps(requestObj)


    try:
        config_obj = configuration.Configuration()
        client_config = config_obj.get_configuration()
        api_instance = ReversalApi(client_config)
        return_data, status, body = api_instance.mit_reversal(requestObj)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)
        return return_data
    except Exception as e:
        write_log_audit(e.status if hasattr(e, 'status') else 999)
        print("\nException when calling ReversalApi->mit_reversal: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    timeout_reversal()
