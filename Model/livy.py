import conf , Log.error_handler as err
import json , requests ,time ,csv , pandas as pd
from random import randint

def start_session():
    # delete_session(0)
    data = {'kind': 'pyspark'}
    response = requests.post(conf.canfra_port + '/sessions', data=json.dumps(data), headers=conf.canfra_headers)
    start_response = response.json()
    session_id = start_response["id"]
    session_state = start_response["state"]
    return session_id, session_state


def get_all_sessions():
    response = requests.get(conf.canfra_port + '/sessions', headers=conf.canfra_headers)
    return response.json()


def delete_session(session_id):
    response = requests.delete(conf.canfra_port + '/sessions/' + str(session_id), headers=conf.canfra_headers)
    return response.json()


def get_session_state(session_id):
    response = requests.get(conf.canfra_port + "/sessions/" + str(session_id), headers=conf.canfra_headers)
    session_response = response.json()
    session_state = session_response["state"]
    return session_state


def get_session(Spark_code):
    print('4/12.Model/livy.get_session             # run it on infra and send answer back')
    try:
        session_id, session_state = start_session()
    except Exception as exc:
        err.handle(102,'','livy is not started or cant start session',)
        raise exc

    while session_state == "starting":
        session_state = get_session_state(session_id)
    response = requests.post(conf.canfra_port + "/sessions/" + str(session_id) + "/statements",
                             data=json.dumps(Spark_code),
                             headers=conf.canfra_headers).json()
    while response["state"] != 'available':
        time.sleep(0.1)
        response = requests.get(conf.canfra_port + "/sessions/" + str(session_id) + "/statements/" + str(response["id"]),
                                 headers=conf.canfra_headers)

        response =response.json()
    #print('\nresponse[state]=',response['state'])

    if response['state'] == "available" :
        if response["output"]["status"] =="error":
            err.handle(103,response["output"]["evalue"],'Error in reading data from Hdfs \n could be becuase of livy','Model/livy.py/line75')
            raise exec
            return {'error':response["output"]["evalue"]}
        else:
            response_dict = response["output"]["data"]
            delete_session(session_id)
            print("     Done! livy step finished")
            df= pd.read_json(response_dict["text/plain"].strip("\\").strip("'"))
            df.to_csv('CRM.csv')
            return df


def main(action , Spark_code ,data ):
    print('3.model/livy.main             # open session for calling infrastructure')
    res = get_session(Spark_code)
    print('5.model/livy.main        # here is the Crm , send it back')
    if "error" not in res.keys():
        return res
    print(' !!!!!!!!!!!!!!!!!!!!!!!!! ')
