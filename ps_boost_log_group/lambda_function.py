import boto3 
import json 
from datetime import datetime
import datetime 
import time
from prettytable import PrettyTable
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def lambda_handeler(event, context):
    log_group_name = "/aws/lambda/ps-boost-prod"

    # Please fill this to recieve the email.
    topic_arn = ""
    subject = "ps-boost-prod Alarm Notification nowfor only 403"

    # For now I have used my email please change this to your email.
    # You can choose not to add from address also
    source_email = "varun.ravikolur@plansource.com"

    # If you find more types of error then add the filter_pattern here, use the status code as the key
    filter_pattern = {}
    filter_pattern[403] = "filter @message like /error/| stats count(*)"

    alarm_name = event['alarmData']['alarmName']

    # The first 3 characters should be the error code status, simalarly keep the alarmName also.
    alarm_name_integer = (int)(alarm_name[:3])

    # End time is the current time in milliseconds
    current_epoch_time_seconds = time.time()
    end_epoc_time = int(current_epoch_time_seconds * 1000)

    # Start time will be the time when the alarm was trigged last i.e the previous state of the alarm tranction. ie. ALARM -> OK
    # Hence from 'OK' to 'ALARM' we are quering.
    # Please Check the event(log) that will be deleiverd when an alarm calls for the lambda function for corresponding parsing of the start_time for log insights.
    event_data = event["alarmData"]
    alarm_data = event_data["previousState"]
    reason_data_str = alarm_data["reasonData"]
    reason_json = json.loads(reason_data_str)
    date_str = reason_json['startDate']
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    epoch_time_seconds = date_obj.timestamp()
    start_epoc_time = int(epoch_time_seconds * 1000)

    try:
        filter_data = filter_events(log_group_name, start_epoc_time, end_epoc_time, filter_pattern[alarm_name_integer])
    except Exception as e:
        logger.error(f"An error occurred while filtering log events: {e}")

    # Since there is nothing in data field only the count will be printed., In case you get data uncomment these two line below.
    # table = generate_table(filter_data)
    # message = f"Log Group name: {log_group_name}\nError status code:{alarm_name_integer}\nData:\n{table}"
    message = f"The count of errors before the previous trigger is:\n{len(filter_data)}"
    try:
        email_response = send_email_via_sns(topic_arn, subject, message, source_email)
    except Exception as e:
        logger.error(f"An error occurred while sending email via SNS: {e}")






# Please do not change this code
def filter_events(log_group_name, start_time = 1719014400000, end_time = 1719100799000, filter_pattern = "fields @timestamp, @message, @logStream, @log| sort @timestamp desc| limit 10000"):
    client = boto3.client('logs')
    
    start_query_response = client.start_query(
        logGroupName=log_group_name,
        startTime=start_time,  
        endTime=end_time,    
        queryString = filter_pattern
    )
    
    query_id = start_query_response['queryId']

    response = None

    while response == None or response['status'] == 'Running':
        # print('Waiting for query to complete ...')
        # Let the tim.sleep be there else you might get stack overflow with the while loop continiuosly running.
        time.sleep(2)
        response = client.get_query_results(
            queryId=query_id
        )
    return response['results']


def generate_table(response):
    table = PrettyTable()
    table.field_names = ['orgID','count']
    count=0
    for i in range(0,len(response)):
        try:
            count+=1
            table.add_row([response[i][0]['value'][:10],response[i][1]['value']])
        except IndexError:
            pass

    return table

 
# Please do not change this code
# def send_email_via_sns(topic_arn, subject, message, source_email):
#     sns = boto3.client('sns')

#     # Create the email message
#     email_message = f"From: {source_email}\nSubject: {subject}\n\n{message}"

#     # Publish the message to the SNS topic
#     response = sns.publish(
#         TopicArn=topic_arn,
#         Message=email_message,
#         Subject=subject,
#         MessageStructure='string'
#     )

#     return response

# Please do not change this code
def sanitize_input(input_str):
    # Remove or replace any potentially dangerous characters or patterns
    sanitized_str = re.sub(r'[\n\r\t\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', input_str)
    return sanitized_str

# Please remove the source email if not needed.
def send_email_via_sns(topic_arn, subject, message, source_email):
    sns = boto3.client('sns')

    # Sanitize inputs
    sanitized_subject = sanitize_input(subject)
    sanitized_message = sanitize_input(message)
    sanitized_source_email = sanitize_input(source_email)

    # Create the email message
    email_message = f"From: {sanitized_source_email}\nSubject: {sanitized_subject}\n\n{sanitized_message}"

    # Publish the message to the SNS topic
    response = sns.publish(
        TopicArn=topic_arn,
        Message=email_message,
        Subject=sanitized_subject,
        MessageStructure='string'
    )

    return response








