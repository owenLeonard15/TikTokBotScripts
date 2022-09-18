import pymongo
from datetime import date, timedelta

# client = pymongo.MongoClient("")   

db = client['TokBotSandbox']

hashtagCollection = db['hashtags']
hashtags = set()
for tagObj in hashtagCollection.find():
    hashtags.add(tagObj['hashtag'])

timedeltas = []
for sub in [1, 7, 14, 30, 180, 365]:
    delta = timedelta(days=sub)
    timedeltas.append(delta)

field_map = ["one_day", "one_week", "two_weeks", "one_month", "six_months", "one_year"]



metricsCollection = db['metrics']

for  tag in hashtags:
    # all metrics for a hashtag
    # only one hashtag at a time to avoid holding too much data locally at once

    # all the percent_change_obj's for a given tag
    result_objects = []
    for metricObj in metricsCollection.find({"hashtag": tag, "date": str(date.today())}):
        # each metricObj will correspond to a new percent change object for that date
        # prevdays is all of the prevdays to find for the current obj
        curDateStr = metricObj["date"]
        curDate = date.fromisoformat(curDateStr)

        db['metric_pct_change'].delete_one({"hashtag": tag, "date": str(date.today())})

        percent_change_obj = {}
        percent_change_obj["hashtag"] = tag
        percent_change_obj["date"] = curDateStr

        prevdays = []
        for delta in timedeltas:
            prevdays.append(str((curDate - delta).isoformat()))
        curviews = metricObj["views"]

        for i in range(len(prevdays)):
            prev_metric_obj = metricsCollection.find_one({"hashtag": tag, "date": prevdays[i]})
            if prev_metric_obj:
                prev_views = prev_metric_obj["views"]
                percent_change = (curviews - prev_views) / prev_views
                percent_change_obj[field_map[i]] = percent_change
        result_objects.append(percent_change_obj)
    # insert all result objects for this tag
    if result_objects:
        db['metric_pct_change'].insert_many(result_objects)
    print("completed insert for hashtag: " + tag)

