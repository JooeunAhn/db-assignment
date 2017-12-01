from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['dit-2']

def embedded_array_data():
    for row in db.athletes.find({"Medals.Medal": "Gold"}):
        print(row)

def projection_and_sort():
    for row in db.athletes.find({"Medals.Medal": "Gold"}, {"Edition": "1896"}).sort("Athlete"):
        print(row)

def aggregation():
    from bson.son import SON
    pipeline = [
    {"$unwind": "$Medal"},
    {"$group": {"_id": "$Medal", "count": {"$sum": 1}}},
    {"$sort": SON([("count", -1), ("_id", -1)])}
    ]
    import pprint
    pprint.pprint(list(db.medals.aggregate(pipeline)))


if __name__ == "__main__":
    aggregation()