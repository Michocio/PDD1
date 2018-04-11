import json

fname = "res.in"
with open(fname) as f:
    content = f.readlines()

content = [x.strip() for x in content]
ile = 0
for i in content:
  arr = i.split("\t")
  if(float(arr[1].split(" ")[0]) >= 0.15):
    #print(float(arr[1].split(" ")[0]))
    #print((arr[0]))
    data = json.loads((arr[0]))
    a = (data['a'])
    b = (data['b'])
    print(a + " " + b)
    af = open(a,"r") 
    bf = open(b,"r")
    #print(af.read())
    #print("___________________")
    #print(bf.read())
    #print("###################")
    ile+=1
  
print(ile)
