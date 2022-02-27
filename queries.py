import pymongo
import re
import json



print("Welcome to pymongo")
client = pymongo.MongoClient("mongodb://localhost:27017/")
print(client)
db = client["envi"]

collection_country = db['countries']
all_countries = collection_country.distinct('country')





################ question 1
print("*"*50)
print("\nQuestion1\n")
substring = ["FR"]
regexp = re.compile(r"|".join(substring), re.IGNORECASE)
substring_match = collection_country.find({"country": regexp },{'_id':0})
print("Countries with substring ",substring[0],":\n")
for c in substring_match:
    print(c)


######### question 2
collection_continent = db['continents_with_country_list']
print("*"*50)


for continents in collection_continent.find():
    for country in all_countries:
        if country in continents['countries']:
            


            #add a new field 'continents' to the existing collection_countries

            collection_country.update_one(
            {"country":country}, 
            {'$set' : {"continent": continents['continent'] }} 
             
            )

print("\nQuestion2 added new field continent and linked it to collection countries\n")


#all_continents = collection_country.distinct('continent')


#################### question 3

#https://stackoverflow.com/questions/36935072/mongodb-get-count-of-common-documents-based-on-the-field-value
print("*"*50)
print("\nQuestion3\n")
print('List of continents with their number of countries')
for count_each in collection_country.aggregate([{
  '$group': {
    '_id': "$continent",
    'countries': {
      '$addToSet': '$country'
    },
    'count': {
      '$sum': 1
    }
  }
}]):
    print(count_each)
    #prints continent,country,number of countries in each continent



#################################question 4
print("*"*50)
print('Question 4\n')
for cont in collection_country.aggregate([{
  '$group': {
    '_id': "$continent",
    'countries': {
      '$addToSet': '$country'
    }
  }
}]):
    
    print("\nContient:",cont['_id'],"\n")
    print("First 4 Countries in alphabetical order\n")
    for sort in sorted(cont['countries'][:4]):
        print(sort)


#################################question 5
print("*"*50)

#load the json file with country and population. I found this on the internet.
json_file = 'country-by-population.json'
f = open(json_file)

# returns JSON object as
# a dictionary
population = json.load(f)

#contains country_by_population as a dictionary

#now let's add this attribute to our collections countries on MongoDB
print(len(population),len(all_countries))
for country in collection_country.find():
    for pop in population:
      if country['country'] == pop['country']:
        print(country['country'],pop['population'])
            


            #add a new field 'continents' to the existing collection_countries

        collection_country.update_one(

        {"country":country['country']},

        {'$set' : {"population": pop['population'] }} 
             
        )









print("\nQuestion5: added new field population and linked it to collection countries\n")


#################################question 6
print("*"*50)
print('Question 6\n')

population_sorted=collection_country.find().sort("population")
print('Countries along with their population in ascending order\n')
for doc in population_sorted:
    print("country:"+doc["country"],"population:",doc["population"])


#################################question 7
print("*"*50)
print('Question 7\n')
print("Countries which have u in their name and population greater than 100000\n")
result=collection_country.find({"country":{"$regex":'u',"$options":'i' } ,"population":{'$gt':100000}})
for doc in result:
    print("country:"+doc["country"],","+"population:",doc["population"])