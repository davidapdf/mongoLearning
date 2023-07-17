from connectMongoDB import Database
from bson import json_util

DB = "zips"
COLLECTION = "zipcodes"

Database.initialize(DB)

query = [
  {
    "$group": {
      "_id": "$state",
      "totalPop": {
        "$sum": "$pop"
      }
    }
  },
  {
    "$match": {
      "totalPop": {
        "$gte": 10 * 1000 * 1000
      }
    }
  }
]

query2 = [
  {
    "$group": {
      "_id": {
        "state": "$state",
        "city": "$city"
      },
      "pop": {
        "$sum": "$pop"
      }
    }
  },
  {
    "$group": {
      "_id": "$_id.state",
      "avgCityPop": {
        "$avg": "$pop"
      }
    }
  }
]
query3 = [
  {
    "$group": {
      "_id": {
        "state": "$state",
        "city": "$city"
      },
      "pop": {
        "$sum": "$pop"
      }
    }
  },
  {
    "$sort": {
      "pop": 1
    }
  },
  {
    "$group": {
      "_id": "$_id.state",
      "biggestCity": {
        "$last": "$_id.city"
      },
      "biggestPop": {
        "$last": "$pop"
      },
      "smallestCity": {
        "$first": "$_id.city"
      },
      "smallestPop": {
        "$first": "$pop"
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "state": "$_id",
      "biggestCity": {
        "name": "$biggestCity",
        "pop": "$biggestPop"
      },
      "smallestCity": {
        "name": "$smallestCity",
        "pop": "$smallestPop"
      }
    }
  }
]
results = Database.aggregate_result(COLLECTION,query3)

obj = list(results)
result_dict = json_util.loads(json_util.dumps(obj))
print(result_dict)