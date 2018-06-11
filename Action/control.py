from kafka import KafkaProducer as kafprod
from kafka import KafkaConsumer as kafcons
import threading , json , re , zlib , time   #public lib
import Model.model as model ,Log.error_handler as err,Cache.Main as cach, conf         #private lib

class connect(object):
    global action, request_id, request_data
    def __init__(self):
        print('8.action/control.connect.init            # start a new thread')
        self.interval = 1
        thread = threading.Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def listener(self):
        print("10.action/control.connect.listener           # Start Analysis server app")
        consumer = kafcons("requests_topic", bootstrap_servers=["10.100.136.40:9092"], auto_offset_reset="latest")
        for request in consumer:
            print('*')
            request_dict = json.loads(request.value)
            action = request_dict['action']
            request_data = ""
            print(action)
            if "data" in request_dict:
                request_data = request_dict['data']
            request_id = request_dict['request_id']
            print('connect.run','     ',action)
            if action not in conf.actionList:
                err.handle(101, data='', message="err101 :action   '" + action + "'  does not exist",
                                     path='Action.control.line23')
            return action,request_id,request_data
        print('#')

    def send_response(self, request_id, result, data=None, message=None):
        response_dict = {'response_id': request_id, 'result': result, 'data': data, 'message': message}
        response_json = json.dumps(response_dict)
        producer = kafprod(bootstrap_servers=[conf.kafkaServer], compression_type='gzip')
        topic = conf.kafka_response_topic
        compressData = conf.kafkaCompressPacket
        if compressData:
            content = str(response_json)
            zcontent = zlib.compress(str.encode(str(content)))
            ret = producer.send(topic, zcontent)
        else:
            response_bytes = str.encode(str(response_json))
            ret = producer.send(topic, response_bytes)
        time.sleep(1)
        return ret


    def run(self):  #, action, request_id, request_data
        print('action/connect.run')
        action1, req_id, req_data = connect.listener(self)
        plot_data = model.main(action1, requestid=req_id, request_data=req_data)
        response = {'plot_data': plot_data, 'ts': None}
        data = re.sub('<script type="text/javascript">', "<script type=\"text/javascript\"> var ts = '" + str(None) + "';" + "var request_id = '" + req_id + "';", plot_data)
        self.send_response(req_id, result=True, data=data, message="")
        return response



def main():
    print('action/control.main            #let cache the Crm and loc')
    cach.load_crm()
    cach.load_loc()
    print('7.action/control.main            #time to listen for appserver request on kafka')
    con1 = connect()
    print(con1)
    while True :
        con1.run()

