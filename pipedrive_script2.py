# python scheduler
import schedule
import time

def job(): # define the whole script as a function

    import requests
    import pandas as pd
    from datetime import datetime
    import pysftp
    import os
    from dotenv import load_dotenv
    load_dotenv()

    today = datetime.today().strftime('%d%m%Y')

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys=None

    # assign env variables
    hostname = os.getenv('hostname')
    username = os.getenv('username')
    password = os.getenv('password')
    token = {'api_token': os.getenv('token')}

    # importing csv file from sftp as dataframe
    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("Ignore the above warning!\n")
        print('Connection succesfully established...')
        with sftp.open(f'Daily Report{today}.csv') as f:
            df = pd.read_csv(f)
            print(f'Successfully read Daily Report{today}.csv!\n')

    print("Cycling through emails in Key2 export doc and seeing which Persons need updating/creating. This may take a while...\n")

    # map k2 doc names to pd field names for better readability (email)
    invoicing_email = 'Accounts Contact.Email Address'
    payroll_email = 'Payroll Contact.Email Address'
    scheme_lead_email = 'Primary Contact.Email Address'
    approver_email = 'HR Contact.Email Address'
    delegated_approver_email = 'Administration Contact.Email Address'

    # for each relevant column in the document, get the email address, and add it to the list
    # then remove duplicates (turn into set)
    emails = []
    for i in range(len(df)):
        loc = df.loc[i]
        emails.append(loc[invoicing_email])
        emails.append(loc[payroll_email])
        emails.append(loc[scheme_lead_email])
        emails.append(loc[approver_email])
        emails.append(loc[delegated_approver_email])
    # turn into set to remove duplicates
    emailset = set(emails)
    # turn back into list so it's hashable
    emailsetlist = list(emailset)
    # remove nan
    del emailsetlist[0]
    # turn to lowercase or else some dupes may remain
    for i in range(len(emailsetlist)):
        emailsetlist[i] = emailsetlist[i].lower()
    # turn to set again to remove dupes
    emailsetlist = set(emailsetlist)
    # turn back to list
    emailsetlist = list(emailsetlist)
    # sort for tidiness
    emailsetlist.sort()


    # map contact type IDs to human readable variables
    invoicing_id = 'pipedrive_field_value'
    payroll_id = 'pipedrive_field_value'
    scheme_lead_id = 'pipedrive_field_value'
    approver_id = 'pipedrive_field_value'
    delegated_approver_id = 'pipedrive_field_value'

    # map k2 doc names to pd field names for better readability (name)
    invoicing_name = 'Accounts Contact.Full Name'
    payroll_name = 'Payroll Contact.Full Name'
    scheme_lead_name = 'Primary Contact.Full Name'
    approver_name = 'HR Contact.Full Name'
    delegated_approver_name = 'Administration Contact.Full Name'
    # org id in df
    k2_org_id = 'Pipedrive Org ID'

    ### map contact type key
    contact_type_key = 'pipedrive_field_key'
    # some more mappings
    name_key = 'pipedrive_field_key'
    email_key = 'pipedrive_field_key'
    notes_key = 'pipedrive_field_key'
    dash_access_key = 'pipedrive_field_key'
    yes = 'pipedrive_field_value'
    org_id_key = 'pipedrive_field_key'

    # initialise list of created/failed Persons
    created_persons = []
    not_created = []

    # initialise list of updated/failed Persons
    updated_persons = []
    not_updated = []

    # cycle through each email in file
    for email in emailsetlist:
        # initiate empty contact type list to hold contact types
        df_contact_type_list = []
        # initiate empty contact name set to hold contact names (automatically removed dupes)
        df_contact_name_set = set()
        
        # append respective contact type and name to list, if email present in relevant column
        
        # for invoicing
        if email in df[invoicing_email].str.lower().values:
            # append to contact_type_list
            df_contact_type_list.append(invoicing_id)
            # get row_num
            row_num = int(df[df[invoicing_email].str.lower() == email].index[0])
            # get corresponding name in relevant column
            contact_name = df[invoicing_name].values[row_num]
            # append to contact_name_list
            df_contact_name_set.add(contact_name)
        # for payroll
        if email in df[payroll_email].str.lower().values:
            df_contact_type_list.append(payroll_id)
            row_num = int(df[df[payroll_email].str.lower() == email].index[0])
            contact_name = df[payroll_name].values[row_num]
            df_contact_name_set.add(contact_name)
        # for scheme_lead 
        if email in df[scheme_lead_email].str.lower().values:
            df_contact_type_list.append(scheme_lead_id)
            row_num = int(df[df[scheme_lead_email].str.lower() == email].index[0])
            contact_name = df[scheme_lead_name].values[row_num]
            df_contact_name_set.add(contact_name)
        # for approver_email    
        if email in df[approver_email].str.lower().values:
            df_contact_type_list.append(approver_id)
            row_num = int(df[df[approver_email].str.lower() == email].index[0])
            contact_name = df[approver_name].values[row_num]
            df_contact_name_set.add(contact_name)
        # for delegated_approver    
        if email in df[delegated_approver_email].str.lower().values:
            df_contact_type_list.append(delegated_approver_id)
            row_num = int(df[df[delegated_approver_email].str.lower() == email].index[0])
            contact_name = df[delegated_approver_name].values[row_num]
            df_contact_name_set.add(contact_name)
        
        # assign an arbitrary name, in case there are multiple names given for the same email address (if there is a name)
        # or assign string in set if len = 1 to variable
        # initiate arbitrary_name and df_contact_name for use in logic later
        arbitrary_name = None
        df_contact_name = None
        if len(df_contact_name_set) > 1:
            arbitrary_name = df_contact_name_set.pop()
        elif len(df_contact_name_set) == 1:
            df_contact_name = df_contact_name_set.pop()
        else:
            put_name = None
            
        # sort df contact type list for later comparison
        df_contact_type_list.sort()
        
        # check if email address already exists in pipedrive
        # params to look for person
        params = {'api_token': os.getenv('token'),'term': email,'fields': 'email','exact_match': 'true'}
        # find Person
        finding_person = requests.get('https://your-domain.pipedrive.com/api/v1/persons/search', params=params)
        try:
            # assign search result to variable
            search_result = finding_person.json()['data']
        except:
            print("Something went wrong while searching for",email)
        
        # if search_result has no items i.e. if no person with that email address is yet in PD
        if len(search_result['items']) == 0:
            pass
            # assign put_name to be not None, if there is one (if not, then sync won't work)
            if df_contact_name == None and arbitrary_name == None:
                print(email,"could not be created because there is no name in df!")
                not_created.append(email)
            elif len(df_contact_name_set) > 0: # if there are still names in df after pop
                put_name = df_contact_name_set.pop()
            else:
                put_name = df_contact_name
                
            # stringify contact types from df
            # create a set (removes any dupes)
            df_contact_type_set = set(df_contact_type_list)
            # convert back to string
            put_contact_types = str(df_contact_type_set).replace("{","").replace("'","").replace("}","")
            
            # get org_id from df
            if str(df[k2_org_id].values[row_num]) == 'nan':
                put_org_id = None
            else:
                put_org_id = int(df[k2_org_id].values[row_num])
            
            # prepare payload for person creation
            data = {
                name_key: put_name,
                email_key: email,
                notes_key: "Created by Key2 sync; email could not be found",
                dash_access_key: yes, # always yes
                contact_type_key: put_contact_types,
                org_id_key: put_org_id
            }
            response = requests.post('https://your-domain.pipedrive.com/api/v1/persons', params=token, data=data)
            # record response
            if response.ok:
                # wait for 2/3 secs to let servers update
                time.sleep(3)
                # get newly created Person ID
                params = {'api_token': os.getenv('token'),'term': email,'fields': 'email','exact_match': 'true'}
                # find Person
                finding_person = requests.get('https://your-domain.pipedrive.com/api/v1/persons/search', params=params)
                # if request was successful
                if finding_person.ok:
                    search_data = finding_person.json()['data']
                    # get Person ID
                    person_id = search_data['items'][0]['item']['id']
                    created_persons.append(person_id)
            else:
                print("Something went wrong while trying to create new Person with email address",email)
                not_created.append(email)
        else: # if there is a person
        
            # number of people in pipedrive with the same email address
            num_of_persons = len(search_result['items'])
            # initiate idx for while loop
            idx = 0

            # reset all vars from previous
            put_name = None
            put_contact_types = None
            put_org_id = None
            put_dash_access = None

            # to stop endless dupe names switching over - initiate variable to hold true if there is a dupe (num_of_persons > 1)
            is_dupe = False
            if num_of_persons > 1:
                is_dupe = True
            
            # while there are still people with the same email address
            while num_of_persons > 0:
                # assign the Person ID to a variable
                person_id = search_result['items'][idx]['item']['id']
                # get the existing Person data
                getting_person = requests.get(f'https://your-domain.pipedrive.com/api/v1/persons/{person_id}',params=token)
                person_data = getting_person.json()['data']
                # increment while loop idx
                idx += 1

                row_num = None

                # for invoicing
                if email in df[invoicing_email].str.lower().values:

                    row_num = int(df[df[invoicing_email].str.lower() == email].index[0])

                # for payroll
                if email in df[payroll_email].str.lower().values:

                    row_num = int(df[df[payroll_email].str.lower() == email].index[0])

                # for scheme_lead 
                if email in df[scheme_lead_email].str.lower().values:

                    row_num = int(df[df[scheme_lead_email].str.lower() == email].index[0])

                # for approver_email    
                if email in df[approver_email].str.lower().values:

                    row_num = int(df[df[approver_email].str.lower() == email].index[0])

                # for delegated_approver    
                if email in df[delegated_approver_email].str.lower().values:

                    row_num = int(df[df[delegated_approver_email].str.lower() == email].index[0])

                #### assign org_id ###
                if person_data['org_id'] != None:
                    existing_org_id = person_data['org_id']['value']
                    put_org_id = None # do nothing
                else: # if there is no org id in pd
                    existing_org_id = None # reset existing_org_id to none
                    if str(df[k2_org_id].values[row_num]) == 'nan': # if there is no org id in k2
                        put_org_id = None # do nothing
                    else: # otherwise assign the relevant org id from key2
                        put_org_id = int(df[k2_org_id].values[row_num]) 
                
                # get existing dashboard access
                existing_dashboard_access = person_data[dash_access_key]
                # if it is blank or it's not "yes", set it to yes. else, keep it.
                if existing_dashboard_access == None or existing_dashboard_access != yes:
                    put_dash_access = yes
                else:
                    put_dash_access = None # None type does not affect PD
                    
                # get existing person name
                existing_person_name = person_data[name_key]
                # if person has more than one email address and already has a name - do nothing
                if len(person_data['email']) > 1 and existing_person_name != None:
                    put_name = None
                # else if person is a dupe and already has a person name - do nothing
                elif is_dupe == True and existing_person_name != None:
                    put_name = None
                else:
                    # if there is no name in key2 - do nothing
                    if df_contact_name == None: 
                        put_name = None
                    # if the names in pd and key2 match - do nothing
                    elif existing_person_name == df_contact_name.strip():
                        put_name = None
                    else: # otherwise, assign ket2 name to PUT
                        put_name = df_contact_name
                    
                # get existing Contact Type/s
                existing_contact_types = person_data[contact_type_key]
                    
                # if there are any contact_types #
                if existing_contact_types is not None:
                    # turn string to list, removing the comma
                    existing_contact_types_list = list(existing_contact_types.split(','))
                    # remove any None values from list
                    existing_contact_types_list = list(filter(None, existing_contact_types_list))
                    # sort for comparisons
                    existing_contact_types_list.sort()
                    # see if it's the same as rb contact types
                    # turn lists into sets for comparisons
                    existing_contact_types_set = set(existing_contact_types_list)
                    df_contact_type_set = set(df_contact_type_list)
                    # if all of df contact types are already in existing contact types or
                    # if all of existing contact types list is identical to df list
                    # do nothing
                    if df_contact_type_set.issubset(existing_contact_types_set) == True \
                    or existing_contact_types_list == df_contact_type_list:
                        put_contact_types = None # do nothing
                    else:    
                        # create a set (removes any dupes)
                        existing_contact_types_set = set(existing_contact_types_list)
                        # merge with df contact_type_list (turn list into set first)
                        df_contact_type_set = set(df_contact_type_list)
                        merged_contact_types_set = existing_contact_types_set.union(df_contact_type_set)
                        # convert back to string
                        put_contact_types = str(merged_contact_types_set).replace("{","").replace("'","").replace("}","")
                # else if there are no contact types in PD but there are in RB #
                elif existing_contact_types is None and df_contact_type_list is not None:
                    # turn df_contact_type_list to string for put
                    df_contact_type_set = set(df_contact_type_list)
                    put_contact_types = str(df_contact_type_set).replace("{","").replace("'","").replace("}","")

                # only initiate put if there is something to put
                if put_contact_types == None and put_org_id == None and put_name == None and put_dash_access == None:
                    pass
                else:
                    # create data payload for put request
                    data = {
                    contact_type_key : put_contact_types,
                    org_id_key : put_org_id,
                    name_key : put_name,
                    dash_access_key : put_dash_access
                    }
                    response = requests.put(f'https://your-domain.pipedrive.com/api/v1/persons/{person_id}',params=token, data=data)
                    if response.ok:
                        print(person_id,"used to be -->","existing contact types:",existing_contact_types,", existing org id:",
                        existing_org_id,", existing name:",existing_person_name,", existing dash access:",existing_dashboard_access,"\n")
                        print(person_id,"is now -->","k2 contact types:",put_contact_types,", k2 org id:",put_org_id,
                        ", k2 name:",put_name,", k2 dash access:",put_dash_access,". NB: if a k2 val is 'None', it means that field was unchanged in PD.\n")
                        updated_persons.append(person_id)
                    else:
                        not_updated.append(person_id)
                    
                # decrement num_of_persons for while loop
                num_of_persons -= 1
                
    ### END OF JOB ###
    print("Job done!\n")

    ### Final print statements + SlackBot ###
    slack_token = os.getenv('slack_password')
    slack_channel = '#script-alerts'
    # create func
    def post_message_to_slack(text):
        return requests.post('https://slack.com/api/chat.postMessage', {
            'token': slack_token,
            'channel': slack_channel,
            'text': text,
        }).json()
    # final prints
    if len(created_persons) > 0:
        if len(created_persons) == 1:
            print("Created the following Person:",created_persons,"\n")
        else:
            created_persons.sort()
            print("Created the following",len(created_persons),"Persons: ",created_persons,"\n")
    else:
        print("No Persons were created!\n")
    
    if len(updated_persons) > 0:
        if len(updated_persons) == 1:
            print("Updated the following Person:",updated_persons,"\n")
        else:
            updated_persons.sort()
            print("Updated the following",len(updated_persons),"Persons: ",updated_persons,"\n")
    else:
        print("No Persons were updated!\n")

    # if anything went wrong
    if len(not_created) > 0:
        if len(not_created) == 1:
            slack_info = f"Unable to create a Person with the following email address from Key2: {not_created}. Need to check script logs on Heroku!"
            post_message_to_slack(slack_info)
            print(slack_info,"\n")
        else:
            not_created.sort()
            slack_info = f"Unable to create Persons with the following {len(not_created)} email addresses from Key2: {not_created}. Need to check script logs on Heroku!"
            post_message_to_slack(slack_info)
            print(slack_info,"\n")

    if len(not_updated) > 0:
        if len(not_updated) == 1:
            slack_info = f"Unable to update the following Person: {not_updated}. Need to check script logs on Heroku!"
            post_message_to_slack(slack_info)
            print(slack_info,"\n")
        else:
            not_updated.sort()
            slack_info = f"Unable to update the following {len(not_updated)} Persons: {not_updated}. Need to check script logs on Heroku!"
            post_message_to_slack(slack_info)
            print(slack_info,"\n")


# run script every day at 18:00
schedule.every().day.at("18:00").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
