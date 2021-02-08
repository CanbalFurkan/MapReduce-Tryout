
from multiprocessing import Process, Value, Array
import os
import math
import time
import zmq
import numpy as np
from abc import ABC, abstractmethod
import re
import sys, getopt


class MapReduce(ABC):
    def __init__(self,NumWorker):
        self.NumWorker=int(NumWorker)

    @abstractmethod
    def Map(self,map_input):
        pass

    @abstractmethod
    def Reduce(self,reduce_input):
        pass

    def _Producer(self,producer_inputer):
        pid = os.getpid()
        context = zmq.Context()
        zmq_socket = context.socket(zmq.PUSH)
        zmq_socket.bind("tcp://127.0.0.1:5500")
        n=self.NumWorker
        for i in range(len(producer_inputer)):
            zmq_socket.send_json([producer_inputer[i]])
            time.sleep(0.25)
        

    def _Consumer(self,i):
        pid = os.getpid()
        context = zmq.Context()
        consumer_receiver = context.socket(zmq.PULL)
        consumer_receiver.connect("tcp://127.0.0.1:5500")

        port_no = 5600+i
        consumer_sender = context.socket(zmq.PUSH)
        consumer_sender.bind("tcp://127.0.0.1:"+str(port_no))
        while True:
            message = consumer_receiver.recv_json()
            res_dict= self.Map(message[0])
            consumer_sender.send_json(res_dict)
        

    def _ResultCollector(self):
        pid = os.getpid()
        context = zmq.Context()
        collector_receiver = context.socket(zmq.PULL)
        i = 0
        my_list=[]
        while True:
            port_no = 5600+i
            collector_receiver.connect("tcp://127.0.0.1:"+str(port_no))
            message = collector_receiver.recv_json()
            my_list.append(message)
            self.Reduce(my_list)
            i=i+1
        

    def start(self,filename):
        print("filename:x ", filename)
        file = open(filename, "r", encoding="utf8")
        text = file.readlines()
        file.close()

        res_collector=Process(target=self._ResultCollector,args=())
        res_collector.start()
        producer = Process(target=self._Producer, args=(text,))
        producer.start()
        consumers=[]

        for i in range(0, self.NumWorker):
            p = Process(target=self._Consumer, args=(i,))
            p.start()
            consumers.append(p)

                
        producer.join()  

        for p in consumers:
            p.terminate()
            p.join()

        
        

        res_collector.terminate()
        res_collector.join()



       






    