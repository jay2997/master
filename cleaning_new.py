import string
import nltk
import time
from nltk.tag import pos_tag
from nltk.corpus import stopwords
import csv
import re
import sys
from bson import json_util
import pymongo
import spacy
import threading 
nlp=spacy.load('en_core_web_sm')
client=pymongo.MongoClient()
db=client['development']
col=db['cleandb']
stop=set(stopwords.words('english'))
files=["Country Variation.txt","Business_model.txt","Commonwords.txt","Jargonwords.txt","Other_Jargon.txt"]
for fi in files:
  with open(fi, "r") as file:
    for jword in file:
      jword=jword.strip('\n')
      stop.add(jword)

print('Corpus loaded')
tags=['JJ','JJS','JJR','NN','NNS','VB','CD']
stop_spacy=['GPE','ORG', 'LANGUAGE', 'NORP']

def cleandate(date):
    date=date.split(' ')
    month = ['January', 'Februray', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October','Novermber','December']
    date[0]=str(1+month.index(date[0]))
    date=date[0]+'.'+date[1]
    pat="%m.%Y"
    epoch = int(time.mktime(time.strptime(date, pat)))
    return epoch


def clean(objects,comma=0):
    temp = []
    if objects is not None:
        objects=nlp(objects)
        for word in objects:
            word=(word.text, word.ent_type_)
            if word[1] not in stop_spacy:
                temp.append(word[0].lower())
    objects=nltk.pos_tag(temp)
    temp = []
    for word in objects:
        if word[0] not in stop and word[1] in tags:
            temp.append(word)
            temp = list(set(temp))
    tokens = []
    for t in temp:
        try:
            string.punctuation.index(t[0])
        except:
            tokens.append(t[0])
    if comma==1:
        tokens = ', '.join(tokens)
    else:
        tokens = ' '.join(tokens)
    return tokens


def writetocol(document):
    db['mldb_temp'].insert(document)
    print('writing batch ')
    return 

start=0
skipdocs=0
counter=0
count = col.find({'site_working':True}).count()
documents=col.find({'site_working':True},no_cursor_timeout=True)
doc_list=[]
for doc in documents:
        counter=counter+1
        print(str(counter)+'/'+str(count),end='\r')
        temp={}
        
        
        try:
            t=doc['current']['category']
        except:
            try:
                t= doc['cuurent']['category']
            except:
                t=''
        if not t:
            temp['category']= ''
        else:
            temp['category'] = clean(t)
        
        try:
            t = doc['current']['category']
        except:
            t= ''
        if not t:
            temp['category_dirty']= ''
        else:
            temp['category_dirty'] = t
        
        
        try:
            t=doc['current']['tags']
        except:
            t=''
        if not t:
            temp['tags_dirty'] = ''
            temp['tags'] = ''
        else:
            t = ', '.join(t)
            temp['tags_dirty']= ''
            temp['tags'] = ''

        
        
        try:
            t=doc['current']['about']
        except:
            t=''
        if not t:
            temp['about'] = ''
        else:
            temp['about']= clean(t)
        
        try:
            t=doc['current']['miscellaneous']['stage']
        except:
            try:
                t = doc['current']['miscellaneous']['status']
            except:
                t=''
        temp['status'] = (t)
        
        try:
            t = doc['current']['location']
        except:
            t = ''
        temp['location'] = (t)
        
        try:
            t = doc['current']['nemp']
        except:
            t = ''
        temp['nemp'] = (t)
        
        try:
            t = doc['current']['funding']
        except:
            t = ''
        temp['funding'] = json_util.dumps(t)
        
        try:
            t = doc['current']['founded']
            temp['founded'] = cleandate(t)
        except:
            t = ''
            temp['founded']=t
        final={}
        final['url']=doc['url']
        try:
            temp['name']=doc['current']['name']
        except:
            temp['name'] = ''
        final['current']=temp
        print(final)
        #doc_list.append(final)
        #if len(doc_list) == 10000:
        #    print('in writing')
        #    t=threading.Thread(target=writetocol,args=(doc_list,))
        #    t.start()
        #    doc_list=[]
    
#t=threading.Thread(target=writetocol,args=(doc_list,))
#t.start()
            
print('over and out!!!')
