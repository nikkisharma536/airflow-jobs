import subprocess
import os
import uuid


def execute_local(args):
    print('running command : %s' % ( ' '.join(args) ))
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = process.communicate()
    print('STDOUT:{}'.format(output))


def del_local_file(list_of_file):
    for file in list_of_file:
        print("Removing file from local")
        os.remove(file)


def read_file(path):
    content = open(path, "r")
    return content.read()


# Create random uuid generator
def generate_uuid():
    return str(uuid.uuid1())

