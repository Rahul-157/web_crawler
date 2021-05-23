#!/usr/env/python

import xlrd 
from bs4 import BeautifulSoup
import requests
import html5lib
import logging
import threading
import re
from urllib.parse import urlparse
import  multiprocessing
import sys
process_url = []

class UrlQueue:
    def __init__(self):
        self.queue = set()
        self.processed = set()
        self._lock = threading.Lock()

    def push(self, url):
        logging.info("Adding %s ", url)
        with self._lock:
            logging.debug("Thread %s has lock", url)
            self.queue.add(url)
            logging.debug("Thread %s about to release lock", url)
        logging.debug("Thread %s after release", url)

    def pop(self):
        logging.info("Removing ")
        with self._lock:
            ret  = self.queue.pop()
            if ret in self.processed:
                return None
            self.processed.add(ret)
            return ret
        
    def size(self):
        with self._lock:
            return len(self.queue)
    
    def get(self): 
        with self._lock:
            return self.queue
        
    def alreadyProcessed(self,url):
        with self._lock:
            return url in self.processed


class Results:
    def __init__(self):
        self.results = dict()
        self._lock = threading.Lock()

    def add(self, key,value):
        logging.info("Adding %s in %s ", value,key)
        with self._lock:
            if self.results.get(key)==None:
                self.results[key] = set()
            self.results[key].add(value)

    def get(self):
        return self.results
    

            
        


def init(filename,col): 
    allowed_domains = set() 
    wb = xlrd.open_workbook(filename)
    sheet = wb.sheet_by_index(0)
    for i in range(1,sheet.nrows):
        url = str.strip(str(sheet.cell_value(i, int(col))))
        if(url !=""):
            allowed_domains.add(url)
    for url in list(allowed_domains)[:multiprocessing.cpu_count()]:
        urlQueue.push(url)




def parse_url():
    globals
    while(urlQueue.size()>0):
        url = urlQueue.pop()
        if url == None:
            continue
        if("http" not in url):
            url = "http://"+ url 
        print(threading.current_thread().name+ " | " +"### "+ url +" ###")
        try:
            r = requests.get(url)
            
            soup = BeautifulSoup(r.content, 'html5lib')
            for link in soup.findAll('a'):
                link = link.get('href')
                if link == None:
                    continue
                if (".pdf"  in link or ".doc"  in link or ".jpg"  in link or  ".png"  in link  or ".jpeg"  in link or ".ppt"   in link or ".csv"  in link or ".xls"  in link or ".txt"  in link):
                    continue
                if (".PDF"  in link or ".DOC"  in link or ".JPG"  in link or  ".PNG"  in link  or ".JPEG"  in link or ".PPT"   in link or ".CSV"  in link or ".XLS"  in link or ".TXT"  in link):
                    continue
                if (url in link):
                    urlQueue.push(link)
                elif ("www" not in link and "http://" not in link and "https" not in link):
                    urlQueue.push(urlparse(url).scheme+"://"+urlparse(url).hostname+'/'+link)


            text = r.text  
            emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", text))
            print(emails)
            results.add(url,",".join(emails))
            with open("results.csv","a+") as f:
                f.write(url+","+",".join(emails)+'\n')
                f.close()
            
        except Exception as error:
            print(error)

if __name__ == "__main__":
    if(len(sys.argv)!=3):
        print("Usage :  python script.py input_file  link_column ")
        exit(1)
    urlQueue =  UrlQueue()   
    results = Results()
    init(sys.argv[1],sys.argv[2])
    t_count = 0
    q_size = urlQueue.size()

    while(q_size>0 and t_count<multiprocessing.cpu_count()):
        t1 = threading.Thread(target=parse_url, args=(),name="# "+ str(t_count))
        t1.start()
        t_count = t_count+1
    t_count=0
    while(q_size>0 and t_count<multiprocessing.cpu_count()):
        t1.join()
        t_count = t_count+1
    parse_url()
    print(results.get())
    print("Done!")
    exit(0)



