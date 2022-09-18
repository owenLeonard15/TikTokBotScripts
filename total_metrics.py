import requests
import os
import logging
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import date, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# def lambda_handler(event=None, context=None):

try:
    # client = MongoClient("")   
    logger.info("Successfully connected to client server")
except Exception:
    logger.info("Unable to connect to the server.")

db = client['TokBotSandbox']
logger.info("connected to db: " + str(db))
# get updated list of hashtags to fetch
hashtagCollection = db['hashtags']
hashtags = set()
for tagObj in hashtagCollection.find():
    hashtags.add(tagObj['hashtag'])
logger.info("hashtags collected")
metrics = []

for tag in ['music']:
    logger.info("collecting soup for tag: " + tag)
    url = 'https://www.tiktok.com/tag/' + tag + '?lang=en'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    if soup is not None:
        logger.info("found soup for tag: " + tag)
    try:
        contents = soup.find('h2', {'title' : 'views'})
        
        try:
            stringNum = contents.text.split(" ")[0]
        except AttributeError: 
            logger.info(tag, " results invalid")
        multiple = 1

        # convert billions and millions to numbers
        if stringNum[-1] == 'B':
            multiple *= 1000000000
            stringNum = stringNum[:-1]
        if stringNum[-1] == 'M':
            multiple *= 1000000
            stringNum = stringNum[:-1]
        if stringNum[-1] == 'K':
            multiple *= 1000
            stringNum = stringNum[:-1]

        # remove decimals
        if "." in stringNum:
            stringNum = stringNum.replace(".", "")
            # if we remove a decimal, the multiple decrease by one factor
            multiple /= 10
        
        metrics.append((tag, int(stringNum) * int(multiple)))
    except TypeError:
        pass
    

# put res into database
tag_documents = [{
    'hashtag': tag,
    'views': int(viewCount),
    'date': str(date.today())
} for tag, viewCount in metrics]

logger.info("Inserting results into DB")
print(tag_documents)
# db['metrics'].insert_many(tag_documents)
logger.info("Succesfully committed results")

client.close()
