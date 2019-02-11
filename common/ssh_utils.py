import paramiko


def execute_remote(key_path, instance_ip, username, cmd_arr):
    key = paramiko.RSAKey.from_private_key_file(key_path)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect/ssh to an instance
    try:
        # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
        client.connect(hostname=instance_ip, username=username, pkey=key)

        # Execute a command(cmd) after connecting/ssh to an instance
        str_cmd = ' '.join(cmd_arr)
        print('Executing remote command: %s' % str_cmd)
        stdin, stdout, stderr = client.exec_command(str_cmd)
        print(stdout.read())

        # close the client connection once the job is done
        client.close()

    except Exception as e:
        print(e)
