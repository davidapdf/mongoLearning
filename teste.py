from connectMongoDB import Database
import json
data = open(r"primer-dataset.json")

Database.initialize("GEO")

dados = Database.find_one("Restaurantes")

