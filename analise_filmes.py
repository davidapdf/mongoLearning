from connectMongoDB import Database
from bson import json_util
from bson import SON



DB = "sample_mflix"
COLLECTION = "movies"

Database.initialize(DB)

query1 = [
    {
        "$lookup": {
            "from": "comments",
            "localField": "_id",
            "foreignField": "movie_id",
            "as": "joined"
        }
    },
    {
        "$project": {
            "id": 1,
            "title": 1,
            "comments.text": 1
        }
    },
    {
        "$limit": 1000
    }
]

query2 = [
  {
    "$match": {
      "plot": {
        "$regex": "baseball",
        "$options": "i"
      }
    }
  },
  {
    "$limit": 5
  },
  {
    "$project": {
      "_id": 0,
      "title": 1,
      "plot": 1
    }
  }
]

query = [
    {
        "$match": {
            "$and": [
                {"plot": {"$regex": "(Hawaii|Alaska)"}},
                {"plot": {"$regex": "\\b[0-9]{4}\\b"}},
                {"genres": {"$nin": ["Comedy", "Romance"]}},
                {"title": {"$nin": ["Beach", "Snow"]}}
            ]
        }
    },
    {
        "$project": {
            "title": 1,
            "plot": 1,
            "genres": 1,
            "_id": 0
        }
    }
]

query = [
    {
        "$match": {
            "title": "The Count of Monte Cristo"
        }
    },
    {
        "$project": {
            "title": 1,
            "year": 1,
            "_id": 0
        }
    }
]

results = Database.aggregate_result(COLLECTION,query)

obj = list(results)
result_dict = json_util.loads(json_util.dumps(obj))
print(result_dict)
