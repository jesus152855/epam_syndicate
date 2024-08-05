import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    start_time = datetime.fromtimestamp(float(event['start_time']))
    end_time = datetime.fromtimestamp(float(event['end_time']))
    print('start_time', start_time)
    print('end_time', end_time)
    client = boto3.client('cloudtrail')
    userListResponse = []
    
    response = client.lookup_events(StartTime=start_time,EndTime=end_time)
    userListResponse = extractUsers(response)
    nextToken = extractNextToken(response)
    isNextToken = isNotBlank(nextToken)
    #isNextToken = False

    while isNextToken:
        response = callCloudTrail(client, start_time, end_time, nextToken)
        userList = extractUsers(response)
        userListResponse.extend(userList);
        nextToken = extractNextToken(response)
        isNextToken = isNotBlank(nextToken)
        
    cleanUserList = set(userListResponse)
    response = sorted(cleanUserList)
    result = json.dumps(response)
    
    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }

    return {
        str(result)
    }
    
def isNotBlank(myString):
    return bool(myString and myString.strip())

def extractNextToken(cloudTrailResponse):
    nextToken = ""
    nextToken = {key: cloudTrailResponse[key] for key in cloudTrailResponse.keys()
       & {'NextToken'}}
    nextTokenList = []
    for i in nextToken.values():
        nextTokenList.append(str(i))
    if(len(nextTokenList) == 1):
        nextToken = nextTokenList[0]
    return nextToken
    
    
def callCloudTrail(client, start_time, end_time, nextToken):
    response = client.lookup_events(StartTime=start_time,EndTime=end_time, NextToken= nextToken)
    return response

def extractUsers(cloudTrailResponse):
    userList = []
    events = {key: cloudTrailResponse[key] for key in cloudTrailResponse.keys()
       & {'Events'}}
    
    # Using for loop
    for i in events.values():
        for j in i: 
            res1 = {key: j[key] for key in j.keys()
                & {'Username', 'userName'}}
            for k in res1.values():
                userList.append(str(k))
    return userList            
    