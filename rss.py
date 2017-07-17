import feedparser
from pymongo import MongoClient
from urlextract import URLExtract
from spacy import load
import listparser
from bs4 import BeautifulSoup


class RSS:
    def __init__(self):
        # Make Connection to MongoDB
        # client = MongoClient()
        client = MongoClient()
        db = client['development']
        coll = db['rss']
        coll_base = db['rss_base']

        # Populate List of URLs from OPML File
        urllist = list()
        for feed in listparser.parse('feedly.opml').feeds:
            urllist.append(feed.url)

        # Make Connection to Spacy
        nlp = load('en')

        # Process URLs one at a time
        for url in urllist:
            try:
                # Pick up feeds using Feedparser
                print('Parsing : ' + url)
                feeds = feedparser.parse(url)

                # Check if field has been updated already
                doc = coll_base.find_one({'url': url})
                if doc and 'updated' in doc and doc['updated'] == feeds.updated:
                    print('Feed Already Up-To-Date')
                else:
                    for entry in feeds.entries:
                        document = dict()
                        document['source'] = url

                        document['links'] = list()
                        document['content'] = None
                        if 'summary' in entry and entry.summary:
                            document['links'] = URLExtract().find_urls(entry.summary)
                            document['content'] = BeautifulSoup(entry.summary, "lxml").get_text()
                            if not document['content'] and 'content' in entry:
                                document['content'] = BeautifulSoup(entry.content, "lxml").get_text()
                        if 'link' in entry and entry.link:
                            document['links'].append(entry.link)

                        document['title'] = entry.title

                        tags = []
                        if 'tags' in entry and entry.tags:
                            for tag in entry.tags:
                                tags.append(tag.term)

                            document['tags'] = tags

                        if 'author' in entry and entry.author:
                            document['author'] = entry.author

                        if 'published' in entry and entry.published:
                            document['published'] = entry.published

                        spacy_tags = dict()
                        orgs = list()
                        if document['title']:
                            for ent in nlp(document['title']).ents:
                                label = ent.label_
                                if label not in spacy_tags:
                                    spacy_tags[label] = list()
                                if label == "ORG":
                                    orgs.append(str(ent))
                                spacy_tags[label].append(str(ent))
                        document['companies_title'] = orgs

                        orgs = list()
                        if document['content']:
                            for ent in nlp(document['content']).ents:
                                label = ent.label_
                                if label not in spacy_tags:
                                    spacy_tags[label] = list()
                                if label == "ORG":
                                    orgs.append(str(ent))
                                spacy_tags[label].append(str(ent))
                        document['companies_content'] = orgs
                        document['spacy_tags'] = spacy_tags

                        if not coll.find_one({'title': entry.title}):
                            coll.insert(document)

                    # Upsert in feed status collection
                    if 'updated' in feeds:
                        coll_base.update_one({'url': url}, {'$set': {'url': url, 'updated': feeds.updated}}, True)
                    else:
                        coll_base.update_one({'url': url}, {'$set': {'url': url}}, True)
            except:
                continue

class RSSTagger:
    def __init__(self):
        # client = MongoClient()
        # db = client['new-db']
        # db2 = client['development']
        client = MongoClient(host="208.113.167.31", port=27017, replicaset='rs0', readPreference='secondaryPreferred')
        db = client['development']
        db.authenticate('sid', 'Sv25074R7uR', mechanism='SCRAM-SHA-1')
        coll = db['rss']
        coll_base = db['rss_base']
        coll_tags = db['mldb']
        tags = []

        result = coll_tags.find({}, {'current.tags': 1, 'current.category': 1})
        for r in result:
            try:
                for s in r['current']['category'].split():
                    if s:
                        tags.append(s)
            except:
                pass
            try:
                for s in r['current']['tags'].split():
                    if s:
                        tags.append(s)
            except:
                pass

        # tags = set(tags)
        # print(tags)

        feeds = coll.find({'new_tags': {'$exists': False}}, {'title': 1, 'content': 1})
        for feed in feeds:
            new_tags = set()
            if feed['content']:
                new_tags = set(tags).intersection(feed['content'].split())
            new_tags = list(set(tags).intersection(feed['title'].split()).union(new_tags))
            coll.update_one({'_id': feed['_id']}, {'$set': {'new_tags': new_tags}})

        company_names = list()
        coll_names = db['cleandb']
        result = coll_names.find({}, {'current.name': 1})
        for r in result:
            if 'current' in r and 'name' in r['current'] and r['current']['name'] and len(r['current']['name']) > 2:
                company_names.append(r['current']['name'])
        # print(company_names)

        # feeds = coll.find({'company_extracted': {'$exists': False}}, {'title': 1, 'content': 1})
        feeds = coll.find({}, {'title': 1, 'content': 1})
        for feed in feeds:
            company_extracted = set()
            if feed['content']:
                company_extracted = set(company_names).intersection(feed['content'].split())
            company_extracted = list(set(company_names).intersection(feed['title'].split()).union(company_extracted))
            coll.update_one({'_id': feed['_id']}, {'$set': {'company_extracted': company_extracted}})
            print(company_extracted)

# The following classes are being called
RSS()
RSSTagger()
