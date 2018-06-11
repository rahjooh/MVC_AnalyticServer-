import sys
from Action import control
from multiprocessing import Process
sys.path.extend(["../"])

if __name__ == '__main__':
    print('0.run')
    proc1 = Process(name='connect', target=control.main)
    proc1.start()
