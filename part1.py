from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['dit-2']

def get_data():
    import csv

    with open("./data.csv", "r") as f:
        reader = csv.DictReader(f)
        rows = []

        while len(rows) < 1000:
            try:
                rows.append(next(reader))
            except(UnicodeDecodeError):
                pass

    return rows


def insert():
    rows = get_data()
    db.winners.insert_many(rows)


if __name__=="__main__":
    insert()
