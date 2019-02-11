# We will use this file for all the
# paths and configs that we will use
# across our project


# Add local path to the PEM file to login to EMR
# Usually the paths to pem should not be exposed
key_path = '/tmp/path/to/key.pem'

# Add IP Address of EMR cluster
# Update this when new cluster is created
ip = '0.0.0.0'

# Added a flag that will be utilized in the operators
# to skip remote/emr commands and perform a dry run
# Will be useful to check the DAG flow and semantic validations
# without actually running commands on EMR
# Set dev_mode = False, for actual prod runs.
dev_mode = True