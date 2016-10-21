import json
from collections import OrderedDict
from random import randint, uniform
import pymongo
import time
from bson.son import SON

polygon = [
        [
            -0.17578125,
            59.085738569819505
        ],
        [
            -0.17578125,
            62.34960927573042
        ],
        [
            7.03125,
            62.34960927573042
        ],
        [
            7.03125,
            59.085738569819505
        ],
        [
            -0.17578125,
            59.085738569819505
        ]
]


def ais_message():
    message = dict()

    pos = [uniform(-180, 180), uniform(-90, 90)]
    message['type'] = 'Feature'
    message["geometry"] = {
        "type": "Point",
        "coordinates": pos
    }

    properties = dict()
    properties['MMSI'] = randint(111111, 999999)

    message['properties'] = properties
    return message


def insert():
    start = time.time()
    print("started")
    for i in range(1000000):

        collection.insert_one(ais_message())

    end = time.time()
    print(end - start)

if __name__ == '__main__':
    client = pymongo.MongoClient("localhost", 27017)
    db = client.test
    collection = db.my_collection
    collection.ensure_index([('geometry', pymongo.GEOSPHERE)])
    # insert()
    max_distance = 10000  # meters

    query = {'geometry': {'$near': SON([('$geometry', SON([('type', 'Point'), ('coordinates', [139, -84])])),
                                     ('$maxDistance', max_distance)])}}

    query_within = {
        'geometry': {
            '$geoWithin': {'$geometry': {'type': 'Polygon', 'coordinates': [polygon]}}
        }
    }

    fc = dict(type='FeatureCollection')
    features = list()
    for doc in collection.find(query_within):
        print(doc)
        del(doc['_id'])
        features.append(doc)

    fc['features'] = features
    print(fc)

    with open('features.json', 'w') as the_file:
        the_file.write(json.dumps(fc))