from pymongo import MongoClient
CONNECTION_STRING = "mongodb+srv://rm430095:JZfKniiKUyT4uEpM@cluster0.ikfvq8m.mongodb.net/"

client = MongoClient(CONNECTION_STRING)
db = client.sample_airbnb

conect = db.listingsAndReviews

query = {
        "price": {"$gt": 100, "$lt": 200},
        "bed_type": "Real Bed"}

results = conect.count_documents(query)
print(results)

