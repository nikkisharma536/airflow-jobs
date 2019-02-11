from airflow.models import BaseOperator
from system_utils import read_file,generate_uuid
from s3_utils import upload_file, delete_file
from ssh_utils import execute_remote
from airflow_job_config import dev_mode


class HiveCustomOperator(BaseOperator):
    template_fields = ('run_date',)

    def __init__(self, script_path, run_date, pem_file_path, emr_master_ip, emr_username='hadoop', *args, **kwargs):
        super(HiveCustomOperator, self).__init__(*args, **kwargs)
        self.script_path = script_path
        self.run_date = run_date
        self.pem_file_path = pem_file_path
        self.emr_master_ip = emr_master_ip
        self.emr_username = emr_username

    def execute(self, context):
        content = read_file(self.script_path)

        # Generate random S3 path for temp scripts
        s3_path = 's3://nikita-ds-playground/scripts/' + generate_uuid() + '.hql'
        cmd_arr = ['hive', '-hiveconf', "RUN_DATE='%s'" % self.run_date, '-f', s3_path]

        if dev_mode:
            print('<dev_mode> Will generate s3 temp script at: %s' % s3_path)
            print('<dev_mode> Will run on EMR: %s' % ' '.join(cmd_arr))
            print('<dev_mode> Will delete s3 temp script at: %s' % s3_path)
        else:
            print('Generating s3 temp script at: %s' % s3_path)
            upload_file(content, s3_path)

            print('Command to run on EMR: %s' % ' '.join(cmd_arr))
            execute_remote(self.pem_file_path, self.emr_master_ip, self.emr_username, cmd_arr)

            print('Deleting s3 temp script at: %s' % s3_path)
            delete_file(s3_path)
