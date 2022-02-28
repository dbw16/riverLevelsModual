import datetime

import boto3
from boto3.dynamodb.conditions import Key

from botocore.exceptions import ClientError
from riverLevels.level import Level

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("river_levels")


def batch_update_level_db(river_name: str, levels: [Level]):
    with table.batch_writer(overwrite_by_pkeys=['river_name', 'timestamp']) as batch:
        for i, level in enumerate(levels):
            print(i)
            batch.put_item(
                Item={
                    'river_name': river_name,
                    'timestamp': int(level.time.timestamp()),
                    'level': level.level,
                }

            )


def update_level_db(river_name: str, level: Level) -> bool:
    try:
        table.put_item(
            Item={
                'river_name': river_name,
                'timestamp': int(level.time.timestamp()),
                'level': level.level,
            },
            ConditionExpression='attribute_not_exists(river_name)'

        )
    except ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        return False
    return True


def get_past_data_dynamo(river_name: str, since_date: datetime.datetime) -> [Level]:
    since_timestamp = int(since_date.timestamp())

    response = table.query(
        KeyConditionExpression=Key('river_name').eq(river_name) & Key('timestamp').gt(since_timestamp)
    )

    return [Level(time=datetime.datetime.fromtimestamp(int(item['timestamp'])), level=item['level']) for item in
            response['Items']]


def get_most_recent_data_dynamo(river_name: str) -> [Level]:
    response = table.query(
        ExpressionAttributeValues=Key('river_name').eq(river_name)
    )
    return [Level(time=datetime.datetime.fromtimestamp(int(item['timestamp'])), level=item['level']) for item in
            response['Items']]


