from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['dit-2']

def one_to_few():
    athlete_list = db.winners.distinct("Athlete")
    new_rows = []
    for ath in athlete_list:
        rows = db.winners.find({"Athlete": ath})
        new_row = {}
        sub_list = []
        for row in rows:
            new_row["Athlete"] = row.pop("Athlete")
            new_row["Gender"] = row.pop("Gender")
            new_row["NOC"] = row.pop("NOC")
            sub_dict = {}
            for key, val in row.items():
                sub_dict[key] = val
            sub_list.append(sub_dict)
        new_row['Medals'] = sub_list
        new_rows.append(new_row)
    db.athletes.insert_many(new_rows)

def one_to_many():
    edition_list = db.winners.distinct("Edition")
    for edi in edition_list:
        rows = db.winners.find({"Edition": edi})
        new_edi = {}
        new_medal_list = []
        for row in rows:
            medal = {}
            new_edi["City"] = row.pop("City")
            new_edi['Edition']= row.pop("Edition")
            for key, val in row.items():
                medal[key] = val
            medal_id = db.medals.insert_one(medal).inserted_id
            new_medal_list.append(medal_id)
        new_edi["Medals"] = new_medal_list
        db.editions.insert_one(new_edi)

def one_to_squillions():
    medal_list = db.winners.distinct("Medal")
    medal_dict = {}
    for medal in medal_list:
        medal_dict[medal] = db.medal_types.insert_one({"Medal": medal}).inserted_id
    for row in db.winners.find():
        new_row = {}
        for key, val in row.items():
            if key == "Medal":
                new_row[key] = medal_dict[val]
            else:
                new_row[key] = val
        db.winners_squ.insert_one(new_row)
