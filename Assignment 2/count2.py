import requests

def my_count():
    response = requests.get('https://storage.googleapis.com/bucket_1019/lines.txt')
    x = response.text

    count = 0

    for i in x.split("\n"):
        count += 1
    
    return "Number of lines is : "+str(count)+ " \n"