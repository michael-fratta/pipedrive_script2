A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that, essentially, updates the relevant entities within a CRM (Pipedrive) with the contents of a CSV file feched from an SFTP server. The steps it follows are explained - concisely - below (see code for full detail):

• connects to an SFTP server, using the pysftp library, and attempts to get the latest file that matches the provided search string

• if a file is found, it iterates through select columns within the file, then their respective rows - and appends its values (email addresses) to a list

• this list is then sorted and shaped in such a way to make it possible to query the Pipedrive API - namely removing duplicates and normalising the strings to lowercase - later

• several mappings (assigning Pipedrive keys or values to a human-readable variable) to the corresponding Pipedrive fields that will later need checking/updating, are then accomplished

• it iterates through this list of emails and, for each email, does the following:

• collects any matching names (within the CSV file - based on row number and column name) and appends them to a list

• removes duplicates 

• connects to the Pipedrive API and looks for a Person entity with a matching email address

• if a Person is not found - a new one is created with the relevant data obtained from the CSV file

• if a Person is found - it checks for any differences between the CSV file and the existing Person entity values: and only updates those that need doing so

• finally - posts any relevant updates/actions to a dedicated Slack (messaging service) channel, as a message, via the Slack API.

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.
