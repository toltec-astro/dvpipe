# LMT data archive user setup scripts

## `00_user_populate.py`

This script reads the user list files, and generate a `user_tokens.csv` file, while doing the following:

* Create users in the dataverse with names being the project IDs.
* For each user, create a group of the same name, and add the user to it.
* Retrieve the initial API_Key and the expiration time for each user
* Save all above to the user_tokens.csv file.

## `10_dataset_role.py`

This is a CLI program that can search dataverse by project ID, then add the user group
of the project ID as member of the datasets.

It can either take a csv file, e.g., the user_tokens.csv generated above, or a list
of project ids in the CLI argument.

## `11_set_roles_all.sh`

This is an example running the `10_dataset_role.py` with the `user_tokens.csv` file.


## Note on the `dv.env` file

It is needed to pass the sensitive information like super user API keys to the programs.

