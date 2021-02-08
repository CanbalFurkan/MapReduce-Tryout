
from FindCitations import FindCitiations
from FindCyclicReferences import FindCyclicReferences
import sys


filename = sys.argv[3]
keyword = sys.argv[1]
number_of_worker=sys.argv[2]

print(keyword)
print(filename)
print(number_of_worker)
if(keyword=="COUNT"):
    x=FindCitiations(number_of_worker)
    x.start(filename)
elif(keyword=="CYCLE"):
    y=FindCyclicReferences(number_of_worker)
    y.start(filename)
else:
    print("No such class")

