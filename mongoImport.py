from connectMongoDB import Database
import json
data = open(r"primer-dataset.json")

Database.initialize()
Database.insertBulk("Restaurantes",data)
