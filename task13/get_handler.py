import json
import boto3
client = boto3.client('dynamodb')


def lambda_handler(event, context):

    uuid = '14ba3d6a-a5ed-491b-a128-0a32b71a38c4'

    if 'headers' in event and 'random-uuid' in event["headers"]:
        uuid += f'-{event["headers"]["random-uuid"]}'


    GetItemProduct = client.get_item(
    TableName='cmtr-eeaff143-dynamodb-l-table-products',
    Key={
        'id': {
          'S': uuid
        }
    })
    itemResult = {key: GetItemProduct[key] for key in GetItemProduct.keys()
       & {'Item'}}

    GetItemStock = client.get_item(
    TableName='cmtr-eeaff143-dynamodb-l-table-stocks',
    Key={
        'product_id': {
          'S': uuid
        }
    })
    
    stockResult = {key: GetItemStock[key] for key in GetItemStock.keys()
       & {'Item'}}
    
    class Response:
        def toJSON(self):
            return json.dumps(
                self,
                default=lambda o: o.__dict__, 
                sort_keys=True,
                indent=4)    
         
    response = Response()     

    for key, value in itemResult.items():
        print(f"{key}: {value}")
        for key, value in value.items():
            setattr(response, key, value)
    for key, value in stockResult.items():
        for key, value in value.items():
            if(key == 'count'):
                setattr(response, key, value)        
        
    return {
      'statusCode': 200,
      'body': response.toJSON()
    }