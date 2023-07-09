import requests

response = requests.get('https://storage.googleapis.com/data_bucket_199/lines.txt')
x = response.text

count = 0

for i in x.split("\n"):
  count += 1

f = open('output.txt', 'w')
f.write('Number of lines is : ' + str(count) + ' \n')
f.close()
