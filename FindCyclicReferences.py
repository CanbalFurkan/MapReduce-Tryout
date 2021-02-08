from abc import ABC, abstractmethod
from MapReduce import MapReduce
import re
import json 
  

class FindCyclicReferences(MapReduce):

    def Map(self,map_input):
       mapper=map_input
       res=re.findall(r'\d+', mapper)
       return {res[0]:res[1]}

    def Reduce(self,reduce_input):
        dict2=dict()
        for x in range(len(reduce_input)-2):
            temp=next((iter)(reduce_input[x]))
            valt=reduce_input[x]
            val=valt[temp]
            for t in range(x+1,len(reduce_input)-1):
                checker_key=next((iter)(reduce_input[t]))
                checker_valt=reduce_input[t]
                checker_val=checker_valt[checker_key]
                if(checker_key==val and checker_val==temp):
                    tubb=(min(val,temp),max(val,temp))
                    if tubb in dict2:
                        dict2[str(tubb)]=dict2[tubb]+1
                        break
                    else :
                        dict2[str(tubb)]=1
                        break


    
        f = open("result.txt", "w")
        f.write(json.dumps(dict2))
        f.close()            


