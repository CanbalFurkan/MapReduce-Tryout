from abc import ABC, abstractmethod
from MapReduce import MapReduce
import re
import json 
  

class FindCitiations(MapReduce):

    def Map(self,map_input):
       mapper=map_input
       res=re.findall(r'\d+', mapper)
       return {res[1]:1 }

    def Reduce(self,reduce_input):
        dict2={}
        for current in reduce_input:
            temp=next((iter)(current))
            if temp in dict2:
                dict2[temp]=dict2[temp]+1             
            else:
                dict2[temp]=1

        f = open("result.txt", "w")
        f.write(json.dumps(dict2))
        f.close()            







