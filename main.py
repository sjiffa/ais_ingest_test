import json
from collections import OrderedDict
from random import randint, uniform
import pymongo
import time
from bson.son import SON
from random import randrange
from datetime import timedelta, datetime

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


def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


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
    d1 = datetime.strptime("2016-10-21 00:00:00", "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime("2016-10-21 23:59:59", "%Y-%m-%d %H:%M:%S")
    properties['time'] = datetime.strftime(random_date(d1, d2), "%Y-%m-%dT%H:%M:%S.%f")
    message['properties'] = properties
    return message


def insert():
    start = time.time()
    print("started insert")
    for i in range(1000000):

        collection.insert_one(ais_message())

    end = time.time()
    print(end - start)


def get(query):
    start = time.time()
    print("started get")

    fc = dict(type='FeatureCollection')
    features = list()
    count = 0
    for doc in collection.find(query):
        del(doc['_id'])
        features.append(doc)
        count += 1

    print("found {count} features!".format(count=count))
    fc['features'] = features

    with open('features.json', 'w') as the_file:
        the_file.write(json.dumps(fc))
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    client = pymongo.MongoClient("localhost", 27017)
    db = client.test
    collection = db['21-10-2016']
    collection.ensure_index([('geometry', pymongo.GEOSPHERE)])
    # insert()
    max_distance = 10000  # meters

    query_near = {'geometry': {'$near': SON([('$geometry', SON([('type', 'Point'), ('coordinates', [139, -84])])),
                                     ('$maxDistance', max_distance)])}}

    query_within = {
        'geometry': {
            '$geoWithin': {'$geometry': {'type': 'Polygon', 'coordinates': [polygon]}}
        }, 'properties.time': {'$gte': '2016-10-21T10:00:00', '$lt': '2016-10-21T11:45:00'}
    }

    get(query=query_within)

