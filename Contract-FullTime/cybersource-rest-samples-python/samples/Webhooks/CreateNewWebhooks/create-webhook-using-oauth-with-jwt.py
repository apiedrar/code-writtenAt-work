from CyberSource import *
import os
import json
from importlib.machinery import SourceFileLoader
from pathlib import Path

config_file = os.path.join(os.getcwd(), "data", "Configuration.py")
configuration = SourceFileLoader("module.name", config_file).load_module()

# To delete None values in Input Request Json body
def del_none(d):
    if isinstance(d, str):
        return d
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
        elif isinstance(value, list):
            for item in value:
                del_none(item)
    return d

def create_webhook_using_oauth_with_jwt():
    name = "My Custom Webhook"
    description = "Sample Webhook from Developer Center"
    organizationId = "<INSERT ORGANIZATION ID HERE>"
    productId = "terminalManagement"

    eventTypes = []
    eventTypes.append("terminalManagement.assignment.update")
    webhookUrl = "https://MyWebhookServer.com:8443/simulateClient"
    healthCheckUrl = "https://MyWebhookServer.com:8443/simulateClientHealthCheck"
    notificationScope = "SELF"
    retryPolicyAlgorithm = "ARITHMETIC"
    retryPolicyFirstRetry = 1
    retryPolicyInterval = 1
    retryPolicyNumberOfRetries = 3
    retryPolicyDeactivateFlag = "false"
    retryPolicyRepeatSequenceCount = 0
    retryPolicyRepeatSequenceWaitTime = 0
    retryPolicy = Notificationsubscriptionsv1webhooksRetryPolicy(
        algorithm = retryPolicyAlgorithm,
        first_retry = retryPolicyFirstRetry,
        interval = retryPolicyInterval,
        number_of_retries = retryPolicyNumberOfRetries,
        deactivate_flag = retryPolicyDeactivateFlag,
        repeat_sequence_count = retryPolicyRepeatSequenceCount,
        repeat_sequence_wait_time = retryPolicyRepeatSequenceWaitTime
    )

    securityPolicySecurityType = "oAuth_JWT"
    securityPolicyProxyType = "external"
    securityPolicyConfigOAuthTokenExpiry = "365"
    securityPolicyConfigOAuthURL = "https://MyWebhookServer.com:8443/oAuthToken"
    securityPolicyConfigOAuthTokenType = "Bearer"
    securityPolicyConfigAdditionalConfigAud = "idp.api.myServer.com"
    securityPolicyConfigAdditionalConfigClientId = "650538A1-7AB0-AD3A-51AB-932ABC57AD70"
    securityPolicyConfigAdditionalConfigKeyId = "y-daaaAVyF0176M7-eAZ34pR9Ts"
    securityPolicyConfigAdditionalConfigScope = "merchantacq:rte:write"
    securityPolicyConfigAdditionalConfig = Notificationsubscriptionsv1webhooksSecurityPolicy1ConfigAdditionalConfig(
        aud = securityPolicyConfigAdditionalConfigAud,
        client_id = securityPolicyConfigAdditionalConfigClientId,
        key_id = securityPolicyConfigAdditionalConfigKeyId,
        scope = securityPolicyConfigAdditionalConfigScope
    )

    securityPolicyConfig = Notificationsubscriptionsv1webhooksSecurityPolicy1Config(
        o_auth_token_expiry = securityPolicyConfigOAuthTokenExpiry,
        o_auth_u_r_l = securityPolicyConfigOAuthURL,
        o_auth_token_type = securityPolicyConfigOAuthTokenType,
        additional_config = securityPolicyConfigAdditionalConfig.__dict__
    )

    securityPolicy = Notificationsubscriptionsv1webhooksSecurityPolicy1(
        security_type = securityPolicySecurityType,
        proxy_type = securityPolicyProxyType,
        config = securityPolicyConfig.__dict__
    )

    requestObj = CreateWebhookRequest(
        name = name,
        description = description,
        organization_id = organizationId,
        product_id = productId,
        event_types = eventTypes,
        webhook_url = webhookUrl,
        health_check_url = healthCheckUrl,
        notification_scope = notificationScope,
        retry_policy = retryPolicy.__dict__,
        security_policy = securityPolicy.__dict__
    )


    requestObj = del_none(requestObj.__dict__)
    requestObj = json.dumps(requestObj)


    try:
        config_obj = configuration.Configuration()
        client_config = config_obj.get_configuration()
        api_instance = CreateNewWebhooksApi(client_config)
        return_data, status, body = api_instance.create_webhook_subscription(requestObj)

        print("\nAPI RESPONSE CODE : ", status)
        print("\nAPI RESPONSE BODY : ", body)

        write_log_audit(status)

        return return_data
    except Exception as e:
        write_log_audit(e.status)
        print("\nException when calling CreateNewWebhooksApi->create_webhook_subscription: %s\n" % e)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    create_webhook_using_oauth_with_jwt()