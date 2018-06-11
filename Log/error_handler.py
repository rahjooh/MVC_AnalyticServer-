import datetime


def handle(errorid,data,message,path):
    now = datetime.datetime.now()

    print( now.year ,  str(now.month), now.day)
    print(now.strftime("%Y-%m-%d"))
    print('\n\nerror','\n\terror id = ',errorid,'\n\tdata =',data, '\n\tmessage = ',message,'\n\tpath =',path)


handle(0,'this is test','test','Log\error_handler')



# 102 livy is not started or cant start session
# 103 Error in reading data from Hdfs
# 104 Error in reading crm data from Hdfs
