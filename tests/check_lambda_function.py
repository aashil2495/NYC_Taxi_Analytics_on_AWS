import pytest
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler

@patch('lambda_function.boto3.client')
@patch('lambda_function.requests.head')
def test_lambda_handler(mock_requests_head, mock_boto3_client):
    # Mocking return values for requests.head
    mock_requests_head.return_value = MagicMock(status_code=200)

    # Mocking return value for boto3.client.put_object
    mock_boto3_client.return_value = MagicMock()

    # Test input data
    event = {'year': '2024', 'month': '12'}
    context = MagicMock()

    # Invoke lambda_handler
    result = lambda_handler(event, context)

    # Assertions
    assert result['result'] == 'success'

    # Ensure boto3.client.put_object is called twice
    assert mock_boto3_client.return_value.put_object.call_count == 2

    

@patch('lambda_function.boto3.client')
@patch('lambda_function.requests.head')
def test_lambda_handler1(mock_requests_head, mock_boto3_client):
    # Mocking return values for requests.head
    # mock_requests_head.return_value = MagicMock(status_code=200)

    # Mocking return value for boto3.client.put_object
    # mock_boto3_client.return_value = MagicMock()

    # Test input data
    event = {'year': '2024', 'month': '12'}
    context = MagicMock()

    # Invoke lambda_handler
    # result = lambda_handler(event, context)

    # Assertions
    

    # Ensure boto3.client.put_object is called twice
    # assert mock_boto3_client.return_value.put_object.call_count == 0

    mock_requests_head.return_value = MagicMock(None)

    # Invoke lambda_handler with the same input data
    result = lambda_handler(event, context)

    # Assertions
    assert result['result'] == 'fail'
 

# Run the tests
if __name__ == "__main__":
    pytest.main()
