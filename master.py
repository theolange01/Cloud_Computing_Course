# Cloud Computing Assignment
# Master script
'''
This is the script for the master instance.
It will only work if the .aws folder of the master instance has been updated beforehand
'''

## Import the libraries

import os 
import sys
import boto3
import numpy as np
import pandas as pd
import time
import regex as re
from tqdm import tqdm


## Boto3 Resource and Client

# ec2 
ec2 = boto3.resource("ec2", region_name='us-east-1')
ec2_client = boto3.client('ec2', region_name='us-east-1')

# sqs
sqs_client = boto3.client('sqs', region_name='us-east-1')
sqs = boto3.resource('sqs', region_name='us-east-1')

# ssm
ssm_client = boto3.client('ssm', region_name='us-east-1')

# s3 bucket
s3_client = boto3.client('s3', region_name='us-east-1')
s3 = boto3.resource('s3', region_name='us-east-1')


## Functions

'''
This function will randomly create the two input matrices given the shape

Input: Two matrix shapes (tuples)
Output: Two input matrices with consistent shapes (numpy.array)
'''
def createMatrices(A_shape, B_shape):
    A = np.random.randint(10,size=A_shape)
    B = np.random.randint(10,size=B_shape)
        
    return A, B        


'''
This function will save the two input matrices and the output matrix on the master ec2 instance. Then these matrices will be uploaded on a s3 buckets

Input: The two input Matrices (A and B) and the output Matrix (C) (numpy.array)
Output: None
'''
def saveMatrixToCsv(A, B, C):
    df_A = pd.DataFrame(A)
    df_A.to_csv('/home/ec2-user/A.csv', header=False, index=False)
        
    df_B = pd.DataFrame(B)
    df_B.to_csv('/home/ec2-user/B.csv', header=False, index=False)
    
    df_C = pd.DataFrame(C)
    df_C.to_csv('/home/ec2-user/C.csv', header=False, index=False)
    
    s3.meta.client.upload_file('/home/ec2-user/A.csv', 'tlbucketcc', 'A.csv')
    s3.meta.client.upload_file('/home/ec2-user/A.csv', 'tlbucketcc', 'B.csv')
    s3.meta.client.upload_file('/home/ec2-user/A.csv', 'tlbucketcc', 'C.csv')


'''
This function will determine the maximum number of rows/columns that can be added in the same work package. It will first the number of elements in the smallest work package possible and then calculate the size of the largest work package possible

Input: The two input matrices and the operation to compute
Output: The maximum number of rows that can be send on a single message
'''
def batchSize(A, B, operation):
    # This integer represents the maximum number of elements that can be sent at once to a SQS Queue
    max_char_per_msg = 257000
    
    
    if operation == 1: # The operation to compute is an addition
        char_per_row = str(A[0].tolist()) + ';' + str(A[0].tolist()) # This would be the smallest work package for an addition
        
        # We determine the maximum number of rows possible for a single message
        max_row_msg = max_char_per_msg//len(char_per_row) 

        
    elif operation == 2: # The operation to compute is a multiplication
        # This is the smallest work package possible for a multiplication
        char_per_row = str(A[0].tolist()) + ';' + str(B[:,0].tolist())
        
        # We determine the maximum number of rows of the first matrix and columns of the second one possible for a second message
        # The same number of rows and columns will be sent
        max_row_msg = max_char_per_msg//len(char_per_row)


    return max_row_msg



'''
This function will ask the output Queue for a message. If a non-empty message is received, it will return its body and receipt handle. Otherwise a default message will be returned

Input: The Url of the Queue that will send the message
Output: The body of the message and the receipt handle
'''
def receive_results (queue_url) :
    # It asks the Queue for a message
    response = sqs_client.receive_message(
                    QueueUrl = queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=5
                )
                
                
    try : # The message is non-empty
        # We retrieve the message body and receipt handle
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        return message['Body'], receipt_handle
        
    except : # The message is empty
        return 'Error', 0


'''
This function will delete a message characterised by its receipt handle from the Queue it came from

Input: The Url of the output Queue and the receipt handle of the message to delete
Output: None
'''
def delete_message(queue_url, receipt_handle):
    # Delete received message from queue
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )



'''
This function will divide the operation into many work packages and send all of them to the input Queue in order to queue the work to the processing nodes

Input: An integer representing the operation to compute, the two input matrices, and the integer that give the largest work package possible
Output: None
'''
def send_msg(operation, A, B, nb_rows_per_msg, total_msg) :
    # First, we retrive the input Queue's Url  
    response = sqs_client.list_queues(
        QueueNamePrefix='JobToWorker',
    )
    
    Url = response['QueueUrls'][0]
    
    nb_msg_sent = 0
    
    # Determine the number of message sent before we update a progress bar
    if total_msg < 1000:
        update_bar = 100
    elif 1000 <= total_msg  < 10000:
        update_bar = 1000
    else :
        update_bar = 5000

    # Create the loading bar
    with tqdm(range(0, total_msg),ncols=100, desc='Sending Messages') as pbar: 

    
        # Then if the operation to compute is an addition
        if operation == 1 : # Matrix Addition 
            # We determine the number of maximum sized messages
            nb_msg = len(A)//nb_rows_per_msg
            
            # We loop over the number of maximum sized messages
            for i in range(nb_msg) :
                # We create the message and send them to the input Queue
                message = str(A[i*nb_rows_per_msg:(i+1)*nb_rows_per_msg].tolist()) + ";" + str(B[i*nb_rows_per_msg:(i+1)*nb_rows_per_msg].tolist()) + ";" + str(i*nb_rows_per_msg) + ';' + str((i+1)*nb_rows_per_msg)
                
                message_sent = sqs_client.send_message(
                                    QueueUrl=Url,
                                    MessageBody = (message),
                                )
                nb_msg_sent += 1
                
                # Update of the progress bar
                if nb_msg_sent % update_bar == 0:
                    print(pbar.update(update_bar))
                elif nb_msg_sent - total_msg == 0:
                    print(pbar.update(total_msg % update_bar))
                
            # If the number of rows of the matrix is not a multiple of the maximum number of rows in a single message, we send a last smaller message
            if len(A)%nb_rows_per_msg :
                # We create the message and send it
                message = str(A[nb_msg*nb_rows_per_msg:].tolist()) + ";" + str(B[nb_msg*nb_rows_per_msg:].tolist()) + ";" + str(nb_msg*nb_rows_per_msg) + ';' + str(len(A))
                
                message_sent = sqs_client.send_message(
                                    QueueUrl=Url,
                                    MessageBody = (message)
                                    )
                                    
                nb_msg_sent += 1
                
            # Update of the progress bar
            if nb_msg_sent % update_bar == 0:
                print(pbar.update(update_bar))
            elif nb_msg_sent - total_msg == 0:
                print(pbar.update(total_msg % update_bar))
        
        
        # If the operation is a multiplication
        elif operation == 2 : # Matrix Multiplication   
            # We determine the number of largest sized messages
            nb_msg_A = len(A)//nb_rows_per_msg
            nb_msg_B = len(B[0])//nb_rows_per_msg
            
            # We loop over these numbers to send all the largest messages
            for i in range (nb_msg_A) :
                for j in range(nb_msg_B):
                    # We create the messages and send them to the input Queue
                    message = str(A[i*nb_rows_per_msg:(i+1)*nb_rows_per_msg].tolist()) + ';' + str(B[:,j*nb_rows_per_msg:(j+1)*nb_rows_per_msg].tolist()) + f';{i*nb_rows_per_msg};{(i+1)*nb_rows_per_msg};{j*nb_rows_per_msg};{(j+1)*nb_rows_per_msg}'
    
                    sqs_client.send_message(QueueUrl=Url,MessageBody = (message))
                    
                    nb_msg_sent += 1
                    
                    # Update of the progress bar
                    if nb_msg_sent % update_bar == 0:
                        print(pbar.update(update_bar))
                    elif nb_msg_sent - total_msg == 0:
                        print(pbar.update(total_msg % update_bar))

                
                # For all blocks of the first input matrix, we need to send the rest of columns of the second input matrix
                if len(B[0])%nb_rows_per_msg :
                    # We create the messages and send them
                    message = str(A[i*nb_rows_per_msg:(i+1)*nb_rows_per_msg].tolist()) + ';' + str(B[:,nb_msg_B*nb_rows_per_msg:].tolist()) + f';{i*nb_rows_per_msg};{(i+1)*nb_rows_per_msg};{nb_msg_B*nb_rows_per_msg};{len(B[0])}'
    
                    sqs_client.send_message(QueueUrl=Url,MessageBody = (message))
                    
                    nb_msg_sent += 1
                    
                    # Update of the progress bar
                    if nb_msg_sent % update_bar == 0:
                        print(pbar.update(update_bar))
                    elif nb_msg_sent - total_msg == 0:
                        print(pbar.update(total_msg % update_bar))

                
            # We need to send the rest of the first input matrix
            if len(A)%nb_rows_per_msg :
                for j in range(nb_msg_B):
                    message = str(A[nb_msg_A*nb_rows_per_msg:].tolist()) + ';' + str(B[:,j*nb_rows_per_msg:(j+1)*nb_rows_per_msg].tolist()) + f';{nb_msg_A*nb_rows_per_msg};{len(A)};{j*nb_rows_per_msg};{(j+1)*nb_rows_per_msg}'
    
                    sqs_client.send_message(QueueUrl=Url,MessageBody = (message))
                    
                    nb_msg_sent += 1
                    
                    # Update of the progress bar
                    if nb_msg_sent % 50 == 0:
                        print(pbar.update(update_bar))
                    elif nb_msg_sent - total_msg == 0:
                        print(pbar.update(total_msg % update_bar))

    
                if len(B[0])%nb_rows_per_msg : 
                    message = str(A[nb_msg_A*nb_rows_per_msg:].tolist()) + ';' + str(B[:,nb_msg_B*nb_rows_per_msg:].tolist()) + f';{nb_msg_A*nb_rows_per_msg};{len(A)};{nb_msg_B*nb_rows_per_msg};{len(B[0])}'
        
                    sqs_client.send_message(QueueUrl=Url,MessageBody = (message))
                    
                    nb_msg_sent += 1
                    
                    
                    # Update of the progress bar
                    if nb_msg_sent % update_bar == 0:
                        print(pbar.update(update_bar))
                    elif nb_msg_sent - total_msg == 0:
                        print(pbar.update(total_msg % update_bar))



'''
This function will receive all the result messages from the output Queue and combine them to create the output matrix

Input: An empty output matrix of the right shape, the number of message expected and the operation to compute
Output: The output matrix C
'''
def recv_msg(C, nbMessageToRecv, operation) :
    # It first need to retrieve the output Queue's Url
    response = sqs_client.list_queues(
        QueueNamePrefix='ResultToMaster',
    )
    
    Url_result = response['QueueUrls'][0]
    
    nb_message_recv = nbMessageToRecv
    
    # Set the parameter to update the progress bar
    if nbMessageToRecv < 1000:
        update_bar = 100
    elif 1000 <= nbMessageToRecv  < 10000:
        update_bar = 1000
    else :
        update_bar = 5000
    
    # Create and define the progress bar
    with tqdm(range(0, nbMessageToRecv),ncols=100, desc='Receiving Messages') as pbar: 

        # While the number of received messages is different from the number of messages expected, we keep asking the output Queue for messages
        while nb_message_recv != 0 :
            # We ask the Queue for a message
            message, receipt_handle = receive_results (Url_result)
            
            # If the message is non-empty
            if message !='Error' :
            
                if operation == 1 : # Matrix addition
                    # It decodes the message and puts the result block at the right position
                    result = message.split(';')
                    
                    matrix_lign = np.array([float(i) for i in re.findall('[-+]?\d+\.?\d*', result[0])])
                    
                    matrix_lign = matrix_lign.reshape(int(result[2]) - int(result[1]), -1)
    
                    C[int(result[1]):int(result[2])] = matrix_lign
    
                elif operation == 2 : # Matrix multiplication
                    # It decodes the message and puts the result block at the right position
                    result = message.split(';')
                    
                    matrix_block = np.array([float(i) for i in re.findall('[-+]?\d+\.?\d*', result[0])])
    
                    matrix_block = matrix_block.reshape(int(result[2])-int(result[1]), int(result[4]) - int(result[3]))
                    C[int(result[1]):int(result[2]), int(result[3]):int(result[4])] = matrix_block
                                
                # Then the message can be deleted from the output Queue
                delete_message(Url_result, receipt_handle)
                
                # We decrease the number of message to receive.
                nb_message_recv -= 1 
                
                # Update of the progress bar
                if  nbMessageToRecv - nb_message_recv == update_bar:
                    print(pbar.update(update_bar))
                    nbMessageToRecv = nb_message_recv
                elif nb_message_recv == 0:
                    print(pbar.update(nbMessageToRecv))
            
    return C



'''
This function will call the two previous function to send all the work packages and receive all the computed results. 


Input: The two input matrices and their shapes, the operation to compute and the integer representing the largest message possible
Output: The output matrix, the time taken to send all the messages and the time taken to receive all the results
'''
def send_recv(A, B, A_shape, B_shape, operation, nb_rows_per_msg):
    # Given the size of each messages, we can determine the total number of messages to create and receive at thee end
    if operation == 1 :
        total_msg = int(np.ceil(len(A)/nb_rows_per_msg))
    
    elif operation == 2:
        total_msg = int(np.ceil(len(A)/nb_rows_per_msg)) * int(np.ceil(len(B[0])/nb_rows_per_msg))
        
    
    send_start = time.time()
    print(f"The operation has been decomposed into {total_msg} work packages")
    print("We begin to send the work packages\n")
    
    # We divide the operation into many work packages and send them to the input Queue
    send_msg(operation, A, B, nb_rows_per_msg, total_msg)
    
    send_end = time.time()
    print("\nAll the work packages have been sent to the Queue")
    print(f"It took {(send_end - send_start):.4f} seconds to send all the work packages")
    
    # We create an empty matrix given the operation and the shapes of the input matrices
    if operation == 1:
        C = np.zeros(A_shape)
    elif operation == 2:
        C = np.zeros((A_shape[0], B_shape[1]))
    
    recv_start = time.time()
    print("The first result can be received\n")
    
    # We receive all the result messages and create the output matrix
    C = recv_msg(C, total_msg, operation)
    
    recv_end = time.time()
    print("\nAll the results have been received")
    print(f"It took {(recv_end-recv_start):.4f} seconds to receive all the results from the Result Queue\n")

    
    return C, send_end - send_start, recv_end - recv_start



'''
This function is the main function of the master instance. It will first create the input matrices to divide them into many work packages that will be sent to the input matrices. 
Then, the function will receive all the results messages to create the output matrix.
Finally, the output matrix will be validated using NumPy

Input: The operatino to compute and the shapes of the two input matrices
Output: None
'''
def master(operation, A_shape, B_shape):
    # First, the input matrices are created given their shapes
    A, B = createMatrices(A_shape, B_shape)
    
    # The size of the work packages are is determined
    nb_rows_per_msg = batchSize(A, B, operation)
    
    startTimeJob = time.time()
    print("The two matrices have been created. The computation can now start")

    # We send the work packages and receive the results afterwards
    C, send_time, recv_time = send_recv(A, B, A_shape, B_shape, operation, nb_rows_per_msg)
    
    endTimeJob = time.time()
    print(f"In total, it took {(endTimeJob - startTimeJob):.4f} seconds to compute the operation")
    print(f"This run time include the sending of all the jobs to the sqs queue, the computation of all the jobs by the worker(s), the reception of all the jobs' result by the master instance and the computation of the final matrix by combining all the results\n")
    
    
    # We print the output matrix and validate our result using NumPy
    print("The output matrix is: ")
    print(C) 
       
    if operation == 1:
        if np.all(A + B == C):
            print("\nAll the elements of the computed matrix using AWS Services are equal to those computed using NumPy")
            print("The computation of a matrix addition is then correct using this application")
        
        else :
            print("\nThe computed matrix does not correspond to the sum of the two input matrices")
            print("Something might have gone wrong, check the AWS interface for more details")
        
    elif operation == 2:
        if np.all(C == A @ B):
            print("\nAll the elements of the computed matrix using AWS Services are equal to those computed using NumPy")
            print("The computation of a matrix multiplication is then correct using this application")
        
        else :
            print("\nThe computed matrix does not correspond to the sum of the two input matrices")
            print("Something might have gone wrong, check the AWS interface for more details")
    
    # The matrices are saved and uploaded on a s3 bucket
    saveMatrixToCsv(A, B, C)
    
    # This final print will be used by the local notebook to make sure that the processing of the operation has ended
    print("Done!")


if __name__ == '__main__':
    master(int(sys.argv[1]), tuple([int(i) for i in re.findall('[-+]?\d+\.?\d*', sys.argv[2])]), tuple([int(i) for i in re.findall('[-+]?\d+\.?\d*', sys.argv[3])]))

