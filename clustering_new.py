import sys
import gensim
from gensim.models import word2vec
import pickle
import numpy
import pymongo
import threading
from sklearn.cluster import KMeans
from sklearn.preprocessing import normalize
import datetime
start_time = datetime.datetime.now()

client=pymongo.MongoClient()
db=client['development']
col=db['mldb_temp']
f=open('vectormodel2.sav','rb')
model=pickle.load(f)
print('Started vectorizing')
data=[]
skipdocs=0

def writetocol(url,clustercurrent):
    col.update_one({"url": url}, {"$set": {"current.cluster": clustercurrent}})
    return

documents=col.find({'current.sf_cat_temp':{'$exists':True}}, no_cursor_timeout = True)
for doc in documents:
        localdata=[]
        try:
            try:
                temp=doc['current']['sf_cat_temp']
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
            if len(hold)<240:
                for x in range(0,(240-len(hold))):
                    hold=hold+(numpy.zeros(shape=(1,))).tolist()
            else:
                hold=hold[0:240]
            for h in hold:
                localdata.append(h)
            try:
                temp=doc['current']['sf_subcat_temp']
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
            if len(hold)<240:
                for x in range(0,(240-len(hold))):
                    hold=hold+(numpy.zeros(shape=(1,))).tolist()
            else:
                hold=hold[0:240]
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
            if len(hold)<240:
                for x in range(0,(240-len(hold))):
                    hold=hold+(numpy.zeros(shape=(1,))).tolist()
            else:
                hold=hold[0:240]
            for h in hold:
                localdata.append(h)
            hold = []
            localdata.append(doc['url'])
            data.append(localdata)
        except:
            skipdocs=skipdocs+1

print(type(data))
print('Vectoriation completed')

urls = []

for i in range(0, len(data)):
    urls.append(data[i].pop(-1))

#print(type(data))

data = numpy.asarray(data, dtype='Float64')
data = normalize(data, norm = 'l2', axis = 0)
print(data[0])
print('Shape of Data is: ' + str(data.shape))
print('Matrix built')
print('clustering started')

with open('kmeans.pickle', 'wb') as handle:
   kmeans = KMeans(n_clusters=100, random_state = 1234)
   pickle.dump(kmeans.fit(data), handle, protocol=pickle.HIGHEST_PROTOCOL)

f=open('kmeans.pickle','rb')
kmeans_model=pickle.load(f)

print('clustering finished')
print('Fitting data with clustering started')

new_2 = kmeans_model.predict(data)
clusterlist= new_2.tolist()
end_time_cluster = datetime.datetime.now()

print(clusterlist)
print('Fitting data with clustering finished')
print('Writing in mongod started')
print('Time taken in vectorization and clustering is: ' + str(end_time_cluster - start_time))

i = 0
for url in urls:
    clustercurrent=clusterlist[i]
    while int(threading.active_count()) > 2000:
        pass
    t = threading.Thread(target=writetocol, args=(url,clustercurrent,))
    t.start()
    i = i+1
end_time = datetime.datetime.now()
print('Total Time taken is: ' + str(end_time-start_time))
print('Task completed')
