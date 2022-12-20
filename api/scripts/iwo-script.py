import logging
from pprint import pformat
import traceback
 
import intersight
import credentials
import json
import time
import pdb
 
FORMAT = '%(asctime)-15s [%(levelname)s] [%(filename)s:%(lineno)s] %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger('openapi')
 
def turboQuery(iwoApiClient, restApiPath, restFunc=None, body=None, noRetry=False, **kwargs):
    ''' Function to send query to IWO
    :param restApiPath: the Rest API address
    :param body: if it is POST operation body needs to be body of the POST
    :param noRetry: if True it will not retry
    :param kwargs: query parameters
    :return: the response from IWO
    '''
    retryNum = 0
    maxRetry = 300
    waitTime = 10
    headerParams = {}
    headerParams['Accept'] = iwoApiClient.select_header_accept(['application/json'])
    headerParams['Content-Type'] = iwoApiClient.select_header_content_type(['application/json'])
    queryParams = []
    authSettings = ['cookieAuth', 'http_signature', 'oAuth2']
    for key, value in kwargs.items():
        queryParams.append((key, value))
    if not restFunc:
        restFunc = 'POST' if body else 'GET'
    while True:
        try:
            response = iwoApiClient.call_api(
                restApiPath, restFunc, header_params=headerParams, query_params=queryParams, body=body,
                auth_settings=authSettings, _return_http_data_only=True, _preload_content=False)
            output = []
            if response.data:
                output = json.loads(response.data)
            return output
        except Exception as exp:
            if retryNum < maxRetry:
                if ('Reason: Service Error' in str(exp)) or ('Bad Gateway' in str(exp)) or \
                    ('Reason: Too Many Requests' in str(exp)) or ('Reason: Internal Server Error' in str(exp)) or \
                    (not noRetry):
                    retryNum = retryNum + 1
                    logger.warning("call_api caused exception {0}".format(str(exp)))
                    logger.warning("Retrying #{0}".format(str(retryNum)))
                    time.sleep(waitTime)
                    continue
            raise
 
 
def main():
    # Configure API key settings for authentication
    apiClient = credentials.config_credentials()
 
#   Example POST call
#     iwoVms = turboQuery(apiClient, '/wo/api/v3/search', body={"className": "VirtualMachine"}, limit=1000)
#     print(iwoVms)
#   Eg2
#     result = turboQuery(apiClient, '/wo/api/v3/groups/?disable_hateoas=true',restFunc='POST',body={"isStatic":True,"displayName":"4 PhysicalMachine Migration Plan (@6qgg2v_8g9bc)","memberUuidList":["74643161502450","74643161502448","74643161502447","74643161502449"],"groupType":"PhysicalMachine","temporary":True}, limit=1000)
#     print(result)
 
#   Example GET call
    #Eg-1 Get IWO targets
    # targets = turboQuery(apiClient, '/wo/api/v3/targets', limit=1000)
    # print(targets)
 
    #Eg-2 Get IWO topology definitions
#     top = turboQuery(apiClient, '/wo/api/v3/topologydefinitions',restFunc='GET', limit=1000)
#     print(top)
 
 
#   Example PUT call changing plan retention period to 90 days
    retentionPeriod = turboQuery(apiClient, '/wo/api/v3/settings/persistencemanager/planRetentionDays',restFunc='PUT',
                                 body={"value":"90","valueObjectType":"String"},limit=1000)
    print(retentionPeriod)
 
 
if __name__ == "__main__":
    main()
