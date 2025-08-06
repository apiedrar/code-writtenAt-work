from authenticationsdk.core.Authorization import *
from authenticationsdk.core.MerchantConfiguration import *
import CyberSource.logging.log_factory as LogFactory
from authenticationsdk.util.PropertiesUtil import *
import authenticationsdk.util.ExceptionAuth
from pathlib import Path

class GetGenerateHeaders:
    def __init__(self):
        # UNIQUE GET ID [EDITABLE]
        self.get_id = "5246387105766473203529"
        # REQUEST TARGET [EDITABLE]
        self.request_target = "/pts/v2/payments/" + self.get_id
        # REQUEST-TYPE [NOT-EDITABLE]
        self.request_type = "GET"
        self.merchant_config = None
        self.date = None

    def get_generate_header(self):
        try:
            util_obj = PropertiesUtil()
            util_obj.cybs_path = os.path.join(os.getcwd(), "samples/authentication/Resources", "cybs.json")
            details_dict1 = util_obj.properties_util()

            mconfig = MerchantConfiguration()
            mconfig.set_merchantconfig(details_dict1)

            mconfig.validate_merchant_details(details_dict1, mconfig)

            self.merchant_config = mconfig
            self.merchant_config.request_host = mconfig.request_host
            self.merchant_config.request_type_method = self.request_type
            mconfig.request_target = self.request_target
            self.date = mconfig.get_time()
            self.get_method_headers()
            write_log_audit(200)
        except ApiException as e:
            print(e)
            write_log_audit(400)
        except KeyError as e:
            print(GlobalLabelParameters.NOT_ENTERED + str(e))
            write_log_audit(400)
        except IOError as e:
            print(GlobalLabelParameters.FILE_NOT_FOUND + str(e.filename))
            write_log_audit(400)
        except Exception as e:
            print(repr(e))
            write_log_audit(400)

    # This method prints values obtained in our code by connecting to AUTH sdk
    def get_method_headers(self):
        logger = LogFactory.setup_logger(self.__class__.__name__, self.merchant_config.log_config)
        try:
            auth = Authorization()
            authentication_type = self.merchant_config.authentication_type

            print("Request Type         :" + self.request_type)
            print(GlobalLabelParameters.CONTENT_TYPE + "         :" + GlobalLabelParameters.APPLICATION_JSON)

            if authentication_type.upper() == GlobalLabelParameters.HTTP.upper():
                print(" " + GlobalLabelParameters.USER_AGENT + "          : " + GlobalLabelParameters.USER_AGENT_VALUE)
                print(" MerchantID          : " + self.merchant_config.merchant_id)
                print(" Date                : " + self.merchant_config.get_time())
    
                temp_sig = auth.get_token(self.merchant_config, self.date)
                print("Signature Header      :" + str(temp_sig))
                print("Host                  :" + self.merchant_config.request_host)
            else:
                temp_sig = auth.get_token(self.merchant_config, self.date)
                print("Authorization Bearer:         " + str(temp_sig.encode("utf-8").decode("utf-8")))
            if self.merchant_config.log_config.enable_log is True:
                logger.info("END> ======================================= ")
                logger.info("\n")
        except ApiException as e:
            authenticationsdk.util.ExceptionAuth.log_exception(logger, e, self.merchant_config)
        except Exception as e:
            authenticationsdk.util.ExceptionAuth.log_exception(logger, repr(e), self.merchant_config)

def write_log_audit(status):
    print(f"[Sample Code Testing] [{Path(__file__).stem}] {status}")

if __name__ == "__main__":
    get_generate_obj = GetGenerateHeaders()
    get_generate_obj.get_generate_header()
