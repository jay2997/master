import datetime
start_time = datetime.datetime.now()
import sys
import pickle
import numpy
import scipy as sp
import pandas as pd
import pymongo
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize
import json
from operator import itemgetter
from threading import Thread
client=pymongo.MongoClient()
db=client['development']
col=db['mldb_temp']
f=open('vectormodel2.sav','rb')
model=pickle.load(f)
print('Started vectorizing')
global mapper
mapper={}
def sim(new_temp):
        global mapper
        for column in new_temp:
            try:
                cosine_list = new_temp.nlargest(11, column)[1:11]
                cosine_list = cosine_list[column]
                cosine_list_2 = cosine_list.to_frame()
                sim_name = list(cosine_list_2.index.values)
                sim_value = list(cosine_list.values)
                final=dict()
                i=0
                similar=[]
                for s in sim_name:
                    temp={}
                    temp['url']=s
                    temp['value']=str(round(sim_value[i]*100, 2)) + '%'
                    temp['name']=mapper[temp['url']]
                    i = i+1
                    similar.append(temp)
                co_name = list(cosine_list_2.columns.values)
                t = Thread(target = write,  args = (similar,co_name,))
                t.start()
            except:
                pass

def write(similar,co_name):
    co_name=str(co_name)[2:-2]
    col.update_one({"url":co_name}, {"$set": {'current.similar': similar}})


def cosine(param):
    global mapper
    mapper={}
    print('Cosine similarity started for clu_no'+str(param))
    data=[]
    skipdocs=0
    documents=col.find({'current.cluster_cat_only_80':param}, no_cursor_timeout = True)
    count = documents.count()
    if count>10:
        for doc in documents:
            localdata=[]
            try:
                #try:
                #    temp=doc['current']['category'].split(' ')
                #    hold=[]
                #    counter=0
                #    for t in temp:
                #        try:
                #            hold=hold+(model[t]).tolist()
                #            counter=counter+1
                #        except:
                #            hold=hold+(numpy.zeros(shape=(20,))).tolist()
                #except:
                #    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                #if len(hold)<100:
                #    for x in range(0,(100-len(hold))):
                #       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                #else:
                #    hold=hold[0:100]
                #for h in hold:
                #    localdata.append(h)
                try:
                    temp=doc['current']['sf_subcat']
                    hold=[]
                    counter=0
                    for t in temp:
                        try:
                            hold=hold+(model[t]).tolist()
                            counter=counter+1
                        except:
                            hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                try:
                    temp=doc['current']['about'].split(' ')
                    hold=[]
                    counter=0
                    for t in temp:
                        try:
                            hold=hold+(model[t]).tolist()
                            counter=counter+1
                        except:
                            hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                try:
                    temp=doc['current']['location'].split(', '),lower()
                    hold=[]
                    counter=0
                    for t in temp:
                        try:
                            hold=hold+(model[t.strip()]).tolist()
                            counter=counter+1
                        except:
                            hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                try:
                    temp=doc['current']['status'].split(' ')
                    hold=[]
                    counter=0
                    for t in temp:
                        try:
                            hold=hold+(model[t]).tolist()
                            counter=counter+1
                        except:
                            hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold +(numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                try:
                    temp=doc['current']['nemp'].split('-')
                    hold=[]
                    counter=0
                    try:
                        hold.append(float(temp[-1]))
                        counter=counter+1
                    except:
                        hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                try:
                    temp=doc['current']['founded']
                    hold=[]
                    counter=0
                    try:
                        hold.append(float(temp))
                        counter=counter+1
                    except:
                        hold=hold+(numpy.zeros(shape=(20,))).tolist()
                except:
                    hold=hold + (numpy.zeros(shape=(20,))).tolist()
                if len(hold)<100:
                    for x in range(0,(100-len(hold))):
                       hold=hold+(numpy.zeros(shape=(1,))).tolist()
                else:
                    hold=hold[0:100]
                for h in hold:
                    localdata.append(h)
                hold = []
                localdata.append(doc['url'])
                data.append(localdata)
                mapper[doc['url']]=doc['current']['name']
            except:
                skipdocs=skipdocs+1
    print('Vectoriation completed')
    urls = []
    for i in range(0, len(data)):
        urls.append(data[i].pop(-1))
    data = numpy.asarray(data, dtype='Float64')
    data = normalize(data, norm = 'l2', axis = 0)
    print(data.shape)
    print('Matrix built')
    start_time_cosine = datetime.datetime.now()
    cosine_matrix = pd.DataFrame(cosine_similarity(data), columns = urls, index = urls)
    print('cosine similarity counted')
    sim(cosine_matrix)


for x in range(0,80):
    cosine(x)

end_time = datetime.datetime.now()
total_time = end_time - start_time
print('Total time taken is:'+str(total_time))
