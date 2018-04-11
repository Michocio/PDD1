# Generate random files to test solution
import csv
import random

with open('tweets1.csv', newline='') as csvfile:
  spamreader = csv.reader(csvfile, delimiter=',')
  i = 0
  for row in spamreader:
    if(random.random() <= 0.010):
      print(row)
      print(row[1])
      name = "fin/" + str(i) + ".in"
      file = open(name, "w+")
      file.write(row[1])
    i+=1

