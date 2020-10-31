import sqlite3
import pandas as pd


class LocalDB:
    def __init__(self):
        # todo: copy abs path of 'RETAIL_PULSE_DB.db' and paste here
        self.conn = sqlite3.connect(
            '/Users/gaurav.mahakud/dev_rrom/retail_pulse/rp_services/RETAIL_PULSE_DB.db')
        self.cursor = self.conn.cursor()

    def load_dataset_to_db(self):
        """create table STORE_DETAILS([StoreID] TEXT,[StoreName] TEXT,[AreaCode] INTEGER);"""
        read_clients = pd.read_csv(r'retail_pulse_assignment_dataset.csv')
        read_clients.to_sql('STORE_DETAILS', self.conn, if_exists='append', index=False)
        self.conn.commit()

    def exec_query(self, query):
        self.cursor.execute(query)
        self.conn.commit()
        result = self.cursor.fetchall()
        return result

    def store_jobs(self, job_id, count, created_time):
        """CREATE TABLE STORE_JOBS ([JobId] INTEGER PRIMARY KEY,[Status] Text,[Count] INTEGER,[CreatedAt] INTEGER)"""
        query = f'''insert into STORE_JOBS (JobId,Status,Count,CreatedAt) values ({job_id},"ongoing",{count},{created_time})'''
        self.cursor.execute(query)
        self.conn.commit()

    def update_job_status(self, job_id, store_id, status, failure="null"):
        query_1 = fr'''update STORE_IMAGES set Status = "{status}",FailReason = "{failure}" where StoreId = "{store_id}";'''
        query_3 = fr'''update STORE_JOBS set Status = "failed" where JobId ={job_id};'''
        # print(query_1, query_3)
        try:
            self.cursor.execute(query_1)
            self.cursor.execute(query_3)
            self.conn.commit()
            print("update Job successful\n")
        except Exception as e:
            print("update_job_status")
            print(e)
        self.cursor.execute("select * from STORE_IMAGES")
        print(self.cursor.fetchall())

    def insert_visit_image(self, job_id, store_id, visit_time):
        query = f'''insert into STORE_IMAGES(StoreID, JobId, Status, VisitTime) values("{store_id}", {job_id}, "ongoing", {visit_time})'''
        # print(query)
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print("db insert successful\n")
        except Exception as e:
            print("insert_visit_image")
            print(e)

    def store_image(self, store_id, perimeter, visit_time):
        """CREATE TABLE STORE_IMAGES ([ID] INTEGER PRIMARY KEY,[StoreID] Text,[JobId] Text,[Status] Text,[Perimeter]
        real,[VisitTime] INTEGER,FailReason TEXT); """

        query_1 = f'''update STORE_IMAGES set Perimeter = {perimeter},Status = "completed" where StoreId = "{store_id}" and VisitTime={visit_time};'''
        try:
            # print(query_1)
            self.cursor.execute(query_1)
            self.conn.commit()
            print("update Image_store successful\n")
        except Exception as e:
            print("store_image")
            print(e)
        self.cursor.execute("select * from STORE_IMAGES")
        print(self.cursor.fetchall())

    def complete_job(self, job_id):
        query_3 = f'''update STORE_JOBS set Status = "completed" where JobId ={job_id};'''
        # print(query_3)
        try:
            self.cursor.execute(query_3)
            self.conn.commit()
            print("update Image_store successful\n")
        except Exception as e:
            print("complete_job")
            print(e)
        self.cursor.execute("select * from STORE_JOBS")
        print(self.cursor.fetchall())

    def fetch_job_status(self, job_id):
        result = self.exec_query(rf"select Status from STORE_JOBS where JobId = {job_id}")
        print(result)
        response = {}
        status_code = 400
        if not result:
            response = {}
            status_code = 400
        elif result[0][0] == 'failed':
            try:
                fail_reason = self.exec_query(
                    rf'select StoreId, FailReason from STORE_IMAGES where JobId = {job_id} and Status = "failed"')
                store_id, error_message = fail_reason[0][0], fail_reason[0][1]
                response = {"status": "failed", "job_id": job_id,
                            "error": [{"store_id": store_id, "error": error_message}]}
                status_code = 200
            except Exception as e:
                print("inside SqliteDB.fetch_job_status", e)
        elif result[0][0] == 'completed':
            response = {"status": "completed", "job_id": job_id}
            status_code = 200
        elif result[0][0] == 'ongoing':
            response = {"status": "ongoing", "job_id": job_id}
            status_code = 200
        return response, status_code

    def fetch_visits(self, store_id, area_code, start_epoch, end_epoch):
        visit_collection = []
        try:
            visit_results = self.exec_query(rf'''select si.StoreId,group_concat(si.Perimeter),
                        group_concat(DATE(ROUND(si.VisitTime / 1000), 'unixepoch')),
                        sd.AreaCode,sd.StoreName from STORE_IMAGES si
                        join STORE_DETAILS sd on si.StoreId = sd.StoreId 
                        where sd.AreaCode={area_code} 
                        and si.VisitTime > {start_epoch} and si.VisitTime < {end_epoch}
                        and si.Status = "completed"
                        group by si.StoreId;''')

            print(visit_results)
            if len(visit_collection) > 0:
                for result in visit_results:
                    perimeters = result[1].split(",")
                    dates = result[2].split(",")
                    data_collector = []
                    for peri, date in zip(perimeters, dates):
                        data_collector.append({"date": date, "perimeter": peri})
                    single_detail = {"store_id": result[0], "area": area_code, "store_name": result[4],
                                     "data": data_collector}
                    visit_collection.append(single_detail)
                response, status_code = {"results": visit_collection}, 200
            else:
                area_existence = self.exec_query(rf'''select AreaCode,group_concat(StoreID) from STORE_DETAILS 
                                                    where AreaCode={area_code} group by AreaCode''')
                print(area_existence)
                if len(area_existence) > 0 and store_id in area_existence[0][1].split(","):
                    response = {"Result": []}, 200
                elif len(area_existence) == 0:
                    response = {"error": "Area_code does not exist in DB"}, 400
                else:
                    response = {"error": "Store_id does not exist in DB"}, 400
            print(visit_collection)

        except Exception as e:
            response = {"error": e}, 400
            print(e)
        return response


if __name__ == '__main__':
    #todo: run this to initiate and check db connection and clear the old rows
    db_obj = LocalDB()
    # create table if you have a new db
    # db_obj.exec_query("""create table STORE_DETAILS([StoreID] TEXT,[StoreName] TEXT,[AreaCode] INTEGER);""")
    # db_obj.exec_query("""CREATE TABLE STORE_JOBS ([JobId] INTEGER PRIMARY KEY,[Status] Text,
    #                     [Count] INTEGER,[CreatedAt] INTEGER)""")
    # db_obj.exec_query("""CREATE TABLE STORE_IMAGES ([ID] INTEGER PRIMARY KEY,[StoreID] Text,[JobId] Text,[Status] Text,[Perimeter]
    # real,[VisitTime] INTEGER,FailReason TEXT); """)

    # db_obj.fetch_visits("S01408724",71000,1599975367000,1605072967000)
    # db_obj.exec_query('drop table STORE_IMAGES')
    # db_obj.exec_query('drop table STORE_JOBS')
    # get_fail = db_obj.exec_query(rf'select StoreId, FailReason from STORE_IMAGES where JobId = 1005 and Status = "failed"')

    try:
        # reset the tables
        db_obj.exec_query("delete from STORE_IMAGES")
        db_obj.exec_query("delete from STORE_JOBS")
        print("DB cleared")

    except Exception as e:
        print(e)
