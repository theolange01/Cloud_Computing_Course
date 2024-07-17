# Cloud Computing Assignment
# Workers source code
'''
This code has to be run on the processing nodes of the AWS Cloud architecture.
It will only work if the .aws folder of the processing nodes has been updated beforehand
'''

## Importation of the useful libraries
import sys
import boto3
import numpy as np
import regex as re

## Boto3 Resource and Client

sqs_client = boto3.client('sqs', region_name='us-east-1')


## Message processing
'''
This function will ask the Input Queue for a message. If a message is received, the function can return the message body and the receipt handle that will be used to delete the message afterwards. If no message can be received, the function will return an string that can be used as a test in the main function

Input: the Url of the input Queue (string)
Output: The body of the message and the receipt handle (string)
'''
def receive_message (queue_url) :
    # We query the SQS Queue for a message
    response = sqs_client.receive_message(
                    QueueUrl = queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=10
                )

    try :
        # If a message has been received, we retrieve the body and the receipt handle to return it
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        return message['Body'], receipt_handle

    except :
        # If the message is empty, we return default outputs for testing afterwards
        return 'Error', 0


'''
This function will delete a message from the Queue specified from its Url given the receipt handle of the message.

Input: The Url of the Input Queue and the receipt handle of the message to delete
Output: None
'''
def delete_message(queue_url, receipt_handle):
    # Delete received message from queue given the queue Url
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


'''
This is the main function of the processing nodes. It will use an infinite while loop to receive messages from the input Queue.
It will first call the receive_message function to receive a message from the Queue. If a message is received, the function will first decode it, then compute the right operation.
Once the result is calculated, it will create the output message and send it to the output Queue.
Then, the function 'delete_message' will delete the processed message from the input Queue.

Input: The operation to compute given as a string
Output: None
'''
def worker(operation) :
    # We first retrieve the Url of both Queues using boto3
    response = sqs_client.list_queues(
        QueueNamePrefix='JobToWorker',
    )

    Url_job = response['QueueUrls'][0]

    response = sqs_client.list_queues(
        QueueNamePrefix='ResultToMaster',
    )

    Url_result = response['QueueUrls'][0]


    # If the operation to compute is an addition
    if operation == 1: # Matrix Addition
        while True : # We use a while loop so that each message from the input Queue has been processed
            # We first receive a message
            message, receipt_handle = receive_message(Url_job)

            if message != 'Error' : # In this case, a non-empty message has been received

                # The message is decoded and the input matrices blocks are re-created
                data = message.split(';')

                lign1 = np.array([int(i) for i in re.findall('[-+]?\d+\.?\d*',data[0])])
                lign2 = np.array([int(i) for i in re.findall('[-+]?\d+\.?\d*', data[1])])

                # The operation is computed
                lign = lign1 + lign2

                # The output message is created and sent to the output Queue
                result = str(lign.tolist()) + ";" + data[2] + ';' + data[3]

                sqs_client.send_message(
                                    QueueUrl=Url_result,
                                    MessageBody = (result)
                                    )

                # Then the message is deleted from the input Queue to ensure that it has been processed
                delete_message(Url_job, receipt_handle)

    # If the operation to compute is a multiplication
    elif operation == 2: # Matrix Multiplication
        while True: # We use a while loop so that each message from the input Queue has been processed
            # We first receive a message
            message, receipt_handle = receive_message(Url_job)

            if message != 'Error' : # In this case, a non-empty message has been received

                # The message is decoded and the input matrices blocks are re-created
                data = message.split(';')

                Block_A = np.array([float(i) for i in re.findall('[-+]?\d+\.?\d*',data[0])])
                Block_A = Block_A.reshape(int(data[3]) - int(data[2]), -1)

                Block_B = np.array([float(i) for i in re.findall('[-+]?\d+\.?\d*',data[1])])
                Block_B = Block_B.reshape(-1, int(data[5]) - int(data[4]))

                # The operation is computed
                Block_result = Block_A @ Block_B

                # The output message is created and sent to the output Queue
                result = str(Block_result.tolist()) + ';' + data[2] + ';' + data[3] + ';' + data[4] + ';' + data[5]

                sqs_client.send_message(
                                    QueueUrl=Url_result,
                                    MessageBody = (result)
                                    )

                # Then the message is deleted from the input Queue to ensure that it has been processed
                delete_message(Url_job, receipt_handle)


if __name__ == '__main__' :
    worker(int(sys.argv[1]))