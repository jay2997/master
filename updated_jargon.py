import pymongo

client = pymongo.MongoClient()
db = client['development']
coll = db['business_analysis']
col = db['mldb_temp']

doc_temp = coll.find_one({'item':"business_analysis"})
value_chain_words = doc_temp['value_chain_words']
value_chain = doc_temp['value_chain']
service_category_words = doc_temp['service_category_words']
service_category = doc_temp['service_category']
customer_category_words = doc_temp['customer_category_words']
customer_category = doc_temp['customer_category']
business_model_words = doc_temp['business_model_words']
business_model = doc_temp['business_model']
status_words = doc_temp['status_words']
status = doc_temp['status']
business_lifecycle_words = doc_temp['business_lifecycle_words']
business_lifecycle = doc_temp['business_lifecycle']
product_type = doc_temp['product_type']


def jargon(category, replacewith, fieldname):
        documents=col.find({'url':"olacabs.com"}, no_cursor_timeout = True)
        for doc in documents:
                f = 0
                try:
                    t = doc['current']['category_dirty'].split(' ')
                    for x in t:
                        if x.lower().strip(',') in category:
                            f = category.index(x.lower().strip(','))
                            
                except:
                	pass

                try:
                    if f == 0:
                        t = doc['current']['tags_dirty'].split(' ')
                        for x in t:
                            if x.lower().strip(',') in category:
                                f = category.index(x.lower().strip(','))
                                
                except:
                    pass

                try:
                    if f == 0:
                        t = doc['current']['about'].split(' ')
                        for x in t:
                            if x in category:
                                f = category.index(x)
                                
                except:
                    pass

                if f > 0:
                    col.update_one({"url": doc['url']}, {"$set": {'current.'+fieldname: replacewith[f]}})
                else:
                	pass
        return
        

def stat(category, replacewith, fieldname):
  documents=col.find({'url':"olacabs.com"}, no_cursor_timeout = True)
  for doc in documents:
                f = 0
                try:
                    t = doc['current']['status'].lower()
                    if t in category:
                        f = category.index(t)
                except:
                    pass
            
                if f > 0:
                    col.update_one({"url": doc['url']}, {"$set": {'current.'+fieldname: replacewith[f]}})
                else:
                	pass
  return

print('1 started')
jargon(value_chain_words, value_chain, 'val_chain')
print('val chain completed')
print('2 started')
jargon(service_category_words, service_category, 'service_cat')
print('service cat completed')
print('3 started')
jargon(customer_category_words, customer_category, 'bmodel_client')
print('bmodel_client_copleted')
print('4 started')
jargon(business_model_words, business_model, 'bmodel')
print('bmodel completed')
print('5 started')
stat(business_lifecycle_words, business_lifecycle, 'business_life')
print('business life completed')
print('6 started')
jargon(product_type, product_type, 'product_type')
print('product type completed')
print('7 started')
stat(status_words, status, 'status_new')
print('status completed completed')
print('Task Completed')
