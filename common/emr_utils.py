from system_utils import read_file,generate_uuid
from s3_utils import upload_file, delete_file
from ssh_utils import execute_remote


def run_on_hive(script_path, run_date, pem_file_path, emr_master_ip, emr_username='hadoop'):
    content = read_file(script_path)

    # Generate random S3 path for temp scripts
    s3_path = 's3://nikita-ds-playground/scripts/' + generate_uuid() + '.hql'
    print('Generating s3 temp script at: %s' % s3_path)
    upload_file(content, s3_path)

    cmd_arr = ['hive', '-hiveconf', "RUN_DATE='%s'" % run_date, '-f', s3_path]
    print('Command to run on EMR: %s' % ' '.join(cmd_arr))
    execute_remote(pem_file_path, emr_master_ip, emr_username, cmd_arr)

    print('Deleting s3 temp script at: %s' % s3_path)
    delete_file(s3_path)
