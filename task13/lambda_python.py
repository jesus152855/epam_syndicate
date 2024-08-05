import json
import boto3
client = boto3.client('dynamodb')

def lambda_handler(event, context):

    uuid = '14ba3d6a-a5ed-491b-a128-0a32b71a38c4'

    if 'headers' in event and 'random-uuid' in event["headers"]:
        uuid += f'-{event["headers"]["random-uuid"]}'

    putItemProduct = client.put_item(
        TableName='cmtr-eeaff143-dynamodb-l-table-products',
        Item={
            'id': {
              'S': uuid
            },
            'title': {
              'S': 'Dummy record'
            },
            'description': {
              'S': 'Hardcoded dynamodb entry'
            },
            'price': {
              'N': '10'
            }
        }
     )
    
    putItemStocks = client.put_item(
        TableName='cmtr-eeaff143-dynamodb-l-table-stocks',
        Item={
            'product_id': {
              'S': uuid
            },
            'count': {
              'N': '1'
            }
        }
    )
    return {
      'statusCode': 200,
      'body': json.dumps(uuid)
    }
