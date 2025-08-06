from CyberSource import *
import os
import json
from importlib.machinery import SourceFileLoader
from pathlib import Path

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

def ebt_electronic_voucher_purchase_from_snap_account_with_visa_platform_connect():
    clientReferenceInformationCode = "EBT - Voucher Purchase From SNAP Account"
    clientReferenceInformation = Ptsv2paymentsClientReferenceInformation(
        code = clientReferenceInformationCode
    )

    processingInformationCapture = False
    processingInformationCommerceIndicator = "retail"
    processingInformationPurchaseOptionsIsElectronicBenefitsTransfer = True
    processingInformationPurchaseOptions = Ptsv2paymentsProcessingInformationPurchaseOptions(
        is_electronic_benefits_transfer = processingInformationPurchaseOptionsIsElectronicBenefitsTransfer
    )

    processingInformationElectronicBenefitsTransferCategory = "FOOD"
    processingInformationElectronicBenefitsTransferVoucherSerialNumber = "123451234512345"
    processingInformationElectronicBenefitsTransfer = Ptsv2paymentsProcessingInformationElectronicBenefitsTransfer(
        category = processingInformationElectronicBenefitsTransferCategory,
        voucher_serial_number = processingInformationElectronicBenefitsTransferVoucherSerialNumber
    )

    processingInformation = Ptsv2paymentsProcessingInformation(
        capture = processingInformationCapture,
        commerce_indicator = processingInformationCommerceIndicator,
        purchase_options = processingInformationPurchaseOptions.__dict__,
        electronic_benefits_transfer = processingInformationElectronicBenefitsTransfer.__dict__
    )

    paymentInformationCardNumber = "4012002000013007"
    paymentInformationCardExpirationMonth = "12"
    paymentInformationCardExpirationYear = "25"
    paymentInformationCard = Ptsv2paymentsPaymentInformationCard(
        number = paymentInformationCardNumber,
        expiration_month = paymentInformationCardExpirationMonth,
        expiration_year = paymentInformationCardExpirationYear
    )

    paymentInformationPaymentTypeName = "CARD"
    paymentInformationPaymentTypeSubTypeName = "DEBIT"
    paymentInformationPaymentType = Ptsv2paymentsPaymentInformationPaymentType(
        name = paymentInformationPaymentTypeName,
        sub_type_name = paymentInformationPaymentTypeSubTypeName
    )

    paymentInformation = Ptsv2paymentsPaymentInformation(
        card = paymentInformationCard.__dict__,
        payment_type = paymentInformationPaymentType.__dict__
    )

    orderInformationAmountDetailsTotalAmount = "103.00"
    orderInformationAmountDetailsCurrency = "USD"
    orderInformationAmountDetails = Ptsv2paymentsOrderInformationAmountDetails(
        total_amount = orderInformationAmountDetailsTotalAmount,
        currency = orderInformationAmountDetailsCurrency
    )

    orderInformation = Ptsv2paymentsOrderInformation(
        amount_details = orderInformationAmountDetails.__dict__
    )

    pointOfSaleInformationEntryMode = "keyed"
    pointOfSaleInformationTerminalCapability = 4
    pointOfSaleInformationTrackData = "%B4111111111111111^JONES/JONES ^3112101976110000868000000?;4111111111111111=16121019761186800000?"
    pointOfSaleInformation = Ptsv2paymentsPointOfSaleInformation(
        entry_mode = pointOfSaleInformationEntryMode,
        terminal_capability = pointOfSaleInformationTerminalCapability,
        track_data = pointOfSaleInformationTrackData
    )

    requestObj = CreatePaymentRequest(
        client_reference_information = clientReferenceInformation.__dict__,
        processing_information = processingInformation.__dict__,
        payment_information = paymentInformation.__dict__,
        order_information = orderInformation.__dict__,
        point_of_sale_information = pointOfSaleInformation.__dict__
    )


    requestObj = del_none(requestObj.__dict__)
    requestObj = json.dumps(requestObj)


    try:
        config_obj = configuration.Configuration()
        client_config = config_obj.get_alternative_configuration()
        api_instance = PaymentsApi(client_config)
        return_data, status, body = api_instance.create_payment(requestObj)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)

        return return_data
    except Exception as e:
        write_log_audit(e.status)
        print("\nException when calling PaymentsApi->create_payment: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    ebt_electronic_voucher_purchase_from_snap_account_with_visa_platform_connect()