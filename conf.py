appserver = "10.100.136.48"

canfra ="10.100.136.40"
canfra_port = "http://10.100.136.40:8070"
canfra_headers = {'Content-Type': 'application/json; charset=utf-8'}
canfra_hdfs_crm = "hdfs://10.100.136.40:9000/user/hduser/pqMer"

PLOT_OUT_DIR = "View/plotsout"

kafkaServer = "10.100.136.40:9092"
kafkaCompressPacket = True   # compress packet to send on kafka or not
kafka_response_topic = "responses_topic"
kafka_request_topic = "requests_topic"

actionList = [
    'guilds_heatmap',
    '3dclustering',
    'no_transaction_vs_amount',
    'no_transaction_vs_harmonic',
    'amount_vs_harmonic',
    'label_counts',
    'no_transactions_statics',
    'amount_statics',
    'harmonic_statics',
]