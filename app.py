import os
import requests
import subprocess
from google.cloud import storage
from flask import Flask
from concurrent.futures import ThreadPoolExecutor

web = Flask(__name__)

#Holds Bucket name entered by user
name = ""

def start_map(num):
    """
    Runs mapper for specified book

    Args:
    num (Int): Book to be mapped
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/mapper"
    parameter = {"int": str(num)}
    return requests.post(url, json=parameter)

def start_shuff(num):
    """
    Runs first part of shuffler for specified book

    Args:
    num (Int): Book to be mapped
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/shuffle1"
    parameter = {"int": str(num)}
    return requests.post(url, json=parameter)

def mid_shuff(num):
    """
    Runs second part of shuffler for specified range of books

    Args:
    num (Int): End book in the range of books to be shuffled
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/shuffle2"
    parameter = {"int": str(num), "neg": "5"}
    return requests.post(url, json=parameter)

def end_shuff(num):
    """
    Runs third part of shuffler for specified range of books

    Args:
    num (Int): End book in the range of books to be shuffled
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/shuffle3"
    parameter = {"int": str(num)}
    return requests.post(url, json=parameter)

def fin_shuff():
    """
    Runs final part of shuffler for specified range of books
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/shuffle4"   
    return requests.post(url)

def shuff_to_red():
    """
    Processes the shuffler output for reducers to improve compute time
    (Splits to word length groups)
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/shuff-red"  
    parameter = {"name" : name}
    return requests.post(url)

def start_redu(num):
    """
    Runs reducer for specified word length groups

    Args:
    num (Int): Word length groups to be reduced
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/reduce"
    parameter = {"int": str(num)}
    return requests.post(url, json=parameter)

def end_redu():
    """
    Takes all the reducer outputs and combines them
  
    Return:
    Returns Http Post Request
    """

    #Calls cloud function via post request and passes json
    url = "https://europe-west2-coursework-367312.cloudfunctions.net/red-out"
    parameter = {"name" : name}
    return requests.post(url, json=parameter)

@web.route("/")
def hello_world():

    name = os.environ.get("NAME", "bruh")

    return "<b>eBook:</b><br> {}".format(name)


if __name__ == "__main__":

    #User enters their bucket name
    name = str(input("Enter Bucket Name:"))

    #Concurrent requests are made to cloud functions when needed
    with ThreadPoolExecutor(max_workers=100) as pool:
        pool.map(start_map,list(range(0,100)))

    with ThreadPoolExecutor(max_workers=100) as pool:
        pool.map(start_shuff,list(range(0,100)))

    with ThreadPoolExecutor(max_workers=20) as pool:
        pool.map(mid_shuff,list(range(5,101,5)))

    with ThreadPoolExecutor(max_workers=5) as pool:
        pool.map(end_shuff,list(range(4,21,4)))

    fin_shuff()

    shuff_to_red()

    with ThreadPoolExecutor(max_workers=40) as pool:
        pool.map(start_redu,list(range(0,38)))

    end_redu()

    # web.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
