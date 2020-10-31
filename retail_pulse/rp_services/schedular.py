import time
from subprocess import PIPE, run
import pickle
from rp_services.sqLiteDB import LocalDB


def cmd(command):
    result = run(command, stdout=PIPE, universal_newlines=True, shell=True)
    return result.stdout


class RegisterJob:

    def __init__(self):
        self.last_job = None
        self.db_obj = LocalDB()

    def check_db_health(self):
        try:
            res = self.db_obj.exec_query("select 1")
            return True
        except Exception as e:
            return e

    def job_seed(self):
        self.last_job = cmd("tail -1 job_seed")
        new_job_id = int(self.last_job) + 1 if self.last_job else 1000
        with open("job_seed", 'a') as register:
            register.write(f"\n{new_job_id}")
        return new_job_id

    def save_job(self,json_data):
        try:
            job_id = self.job_seed()
            count = json_data['count']
            created_at = round(time.time()*1000)
            file = f'saved_jobs/Job_{job_id}.pickle'
            job_json = {"job_id":job_id,"visits":json_data["visits"]}
            with open(file, 'wb') as data_file:
                pickle.dump(job_json, data_file, protocol=pickle.HIGHEST_PROTOCOL)
            self.db_obj.store_jobs(job_id, count, created_at)
            print(self.db_obj.exec_query("select * from STORE_JOBS"), end="\n")
            print(self.db_obj.exec_query("select * from STORE_IMAGES"), end="\n")
            return True,job_id
        except Exception as e:
            print("----",e)
            return "Failed to save job"

    def show_tables(self):
        print(self.db_obj.exec_query("select * from STORE_JOBS"),end="\n")
        print(self.db_obj.exec_query("select * from STORE_IMAGES"),end="\n")

    def get_job_status(self,jobid):
        try:
            result, status_code = self.db_obj.fetch_job_status(jobid)
            return result, status_code
        except Exception as e:
            print(e)

    def get_visit_info(self,store_id,area,start_epoch,end_epoch):
        try:
            result, status_code = self.db_obj.fetch_visits(store_id,area,start_epoch,end_epoch)
            return result, status_code
        except Exception as e:
            print(e)


if __name__ == "__main__":

    reg_obj = RegisterJob()
    reg_obj.get_job_status(1005)
    print(reg_obj.check_db_health())