import hashlib
import uuid
import pandas as pd
# Import appropriate modules from the client library.
from googleads import adwords

auth_file_path = './googleads_sk.yaml'
email_path = './emails.csv'

def main(client):
  # Initialize appropriate services.
  user_list_service = client.GetService('AdwordsUserListService', 'v201809')

  user_list = {
      'xsi_type': 'CrmBasedUserList',
      'name': 'Customer relationship management list #%d' % uuid.uuid4(),
      'description': 'A list of customers that originated from email addresses',
      # CRM-based user lists can use a membershipLifeSpan of 10000 to indicate
      # unlimited; otherwise normal values apply.
      'membershipLifeSpan': 30,
      'uploadKeyType': 'CONTACT_INFO'
  }

  # Create an operation to add the user list.
  operations = [{
      'operator': 'ADD',
      'operand': user_list
  }]

  result = user_list_service.mutate(operations)
  user_list_id = result['value'][0]['id']

  df = pd.read_csv(email_path, usecols=['Email_address_hash'], chunksize=3000, nrows=9000)
  for emails in df:
    emails = set(emails['Email_address_hash'])
    members = [{'hashedEmail': email} for email in emails]
    upload_emails(user_list_service, user_list_id, members)

def upload_emails(service, user_list_id, email_list):
  mutate_members_operation = {
      'operand': {
          'userListId': user_list_id,
          'membersList': email_list
      },
      'operator': 'ADD'
  }
  response = service.mutateMembers([mutate_members_operation])
  if 'userLists' in response:
    for user_list in response['userLists']:
      print('Uploaded %i items to user list with name "%s" and ID "%d"'
            % (len(email_list), user_list['name'], user_list['id']))


if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage(auth_file_path)
  main(adwords_client)
