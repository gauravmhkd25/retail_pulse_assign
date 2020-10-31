from PIL import Image
import urllib.request as ur
import urllib.error
import pickle
import os
import time
from rp_services.sqLiteDB import LocalDB


class ImageProcessor:

    def __init__(self):
        self.data = None
        self.db_obj = LocalDB()

    def load_from_pickle(self, pickle_file):
        try:
            with open(pickle_file, 'rb') as data_file:
                self.data = pickle.load(data_file)
        except Exception as e:
            print(e)

    def process_data(self):
        job_id = self.data['job_id']
        for data in self.data['visits']:
            store_Id = data['store_id']
            visit_time = data['visit_time']
            if not os.path.exists(f"../rp_services/images/{store_Id}"):
                os.mkdir(f"../rp_services/images/{store_Id}")
            perimeter = 0.0
            self.db_obj.insert_visit_image(job_id, store_Id, visit_time)
            try:
                for count, image in enumerate(data['image_url']):
                    try:
                        #print(count, image)
                        img_download = ur.urlretrieve(image, f"../rp_services/images/{store_Id}/{store_Id}_{count}.jpg")
                        im = Image.open(img_download[0])
                        width, height = im.size
                        perimeter += 2 * (width + height)
                        time.sleep(150/1000)

                    except urllib.error.HTTPError as e1:
                        raise Exception(f"Download Error image- {image.strip('https://')}, Exception - {e1}")
                    except Exception as e2:
                        raise Exception(f"Processing Error image- {image.strip('https://')}, Exception  - {e2}")

                self.db_obj.store_image(store_Id, perimeter, visit_time)
            except Exception as e3:
                print("caught Processor Exception")
                self.db_obj.update_job_status(job_id, store_Id, "failed", e3)
                print(e3)
                return False

        self.db_obj.complete_job(job_id)
        return True


if __name__ == '__main__':
    ip_object = ImageProcessor()
    print("Job Processor started")
    while True:  # to replicate a running server
        refresh_current_jobs = [job_json for job_json in os.listdir("saved_jobs") if
                                os.path.isfile(os.path.join("saved_jobs", job_json))]
        for job_json in refresh_current_jobs:
            try:
                print(f"\n=====starting With Job {job_json}======")
                ip_object.load_from_pickle(f"saved_jobs/{job_json}")
                job_completed = ip_object.process_data()
                print(f"{job_json} == {job_completed}")
                refresh_current_jobs.remove(job_json)
            except Exception as e:
                print(e)
                refresh_current_jobs.remove(job_json)
                continue
            current_file_path = os.path.join("saved_jobs", job_json)
            os.remove(current_file_path)

        time.sleep(1)
