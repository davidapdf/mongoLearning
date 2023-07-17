from connectMongoDB import Database

data = open(r"zips.json")

Database.initialize("zips")
Database.insertBulk("zipcodes",data)
