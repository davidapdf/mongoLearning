from connectMongoDB import Database
from bson import json_util

DB = "sample_airbnb"
COLLECTION = "listingsAndReviews"
Database.initialize(DB)

#parte 1 e 2
# collections = Database.list_collection()
# for i in collections:
#     print(i)
# j = Database.find_one(COLLECTION)
# print(j)

#Parte 3
# query = {"price": {"$gt": 100, "$lt": 200}, "bed_type": "Real Bed"}
query2 = {"price": {"$gt":100, "$lt": 200},"bed_type": "Real Bed"}

#parte4
query4 = [{
     "$group": {"_id": "$beds",
     "avg_by_beds": { "$min": "$price"}
     }
   },
   { "$sort": { "avg_by_beds": 1 } }]
# results = Database.aggregate_result(COLLECTION,query4)

query5 = [
    {
        "$group":{"_id":"$bed_type",
                  "avg_by_bed_type": {"$avg":"$price"}}
    }
]

query6 = [   
   {     
     "$group": {
        "_id": "$address.suburb",
        "avg_by_suburb": { "$avg": "$price" }
              }   
    } ] 

query7 = [ 
   {
     "$group" : 
     { 
         "_id":{
              "suburb":"$address.suburb", "bedrooms":"$bedrooms"
             }, 
         "count":{"$sum":1}, 
         "avg":{"$avg":"$price"}
      } 
    } ]

query8 = [ 
   { 
     "$match" : {"amenities": "Pool"} 
   }, 
   {
    "$group" : 
    {
      "_id":{
           "suburb":"$address.suburb","bedrooms":"$bedrooms"
          }, 
      "count":{"$sum":1}, 
      "avg":{"$avg":"$price"}
    } 
   } ]

query9 = [
   {
     "$match": { "amenities": 
                { "$in": ["Cable TV", "Garden or backyard", "Coffee maker"] 
                } 
               } 
     }, 
   { 
     "$project": { "scores": 
                        { "$objectToArray": "$review_scores" 
                        } 
                } 
    },
   { 
    "$unwind": "$scores" 
   },  
   { 
    "$group": { "_id": "$scores.k", "average_score": { "$avg": "$scores.v" } } 
   } ]

query10 = [   
   { 
    "$match": {   
             "$and": [
                   {"amenities": { "$in": ["Pool", "Air conditioning", "Wifi"] }
                   },
                   {"price": { "$lt": 200 } 
                   },
                   {"address.suburb": "Copacabana"}
                   ]  
              }
    },   
    { 
     "$project": { 
                "scores": { "$objectToArray": "$review_scores" } 
               } 
     },
    { "$unwind": "$scores" },
   { 
     "$group": { 
              "_id": "$scores.k", "average_score": { "$avg": "$scores.v" } 
             } 
   } ]

query11 = [   
   { 
    "$match": {   
             "$and": [
                   {"amenities": { "$in": ["Pool", "Air conditioning", "Wifi"] }
                   },
                   {"price": { "$lt": 200 } 
                   },
                   {"address.suburb": "Ipanema"}
                   ]  
              }
    },   
    { 
     "$project": { 
                "scores": { "$objectToArray": "$review_scores" } 
               } 
     },
    { "$unwind": "$scores" },
   { 
     "$group": { 
              "_id": "$scores.k", "average_score": { "$avg": "$scores.v" } 
             } 
   } ]

query12 = [
  {
    "$match": {
      "reviews.reviewer_id": "51483096"
    }
  },
  {
    "$unwind": "$reviews"
  },
  {
    "$graphLookup": {
      "from": "listingsAndReviews",
      "startWith": "$reviews.reviewer_id",
      "connectFromField": "reviews.reviewer_id",
      "connectToField": "reviews.reviewer_id",
      "as": "reviewed_listings",
      "maxDepth": 1,
      "depthField": "depth",
      "restrictSearchWithMatch": {
        "reviews.reviewer_id": {
          "$ne": "51483096"
        }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "listing": "$_id",
      "url": "$listing_url"
    }
  }
]

query13 = [
  {
    "$match": {
      "host.host_id": "51399391"
    }
  },
  {
    "$graphLookup": {
      "from": "listingsAndReviews",
      "startWith": "$host.host_id",
      "connectFromField": "host.host_id",
      "connectToField": "host.host_id",
      "as": "multi-listing-hosts"
    }
  }
]

questao = "Número de propriedades e o preço médio por noite"
results = Database.aggregate_result(COLLECTION,query13)
obj = list(results)
result_dict = json_util.loads(json_util.dumps(obj))
print(f'{questao}')
print("qtd: ", len(result_dict))
print("---------")
print(result_dict)