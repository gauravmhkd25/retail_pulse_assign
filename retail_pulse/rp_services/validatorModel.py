

class JobValidator:
    def __init__(self,json_payload):
        self.payload_to_validate = json_payload
        self.count_visit = None
        self.count = 0
        self.visits = None
        self.cur_store_id = "at Pos 1" # to keep a check on missing store ids

    def check_visits(self):
        print("here I am ")
        try:
            print(self.payload_to_validate.keys())
            self.count = self.payload_to_validate["count"] if "count" in self.payload_to_validate.keys() else None
            self.visits = self.payload_to_validate["visits"] if "visits" in self.payload_to_validate.keys() else None
            if self.visits and self.count:
                if len(self.visits) == self.count:
                    self.count_visit = response = True
                else:
                    self.count_visit = False
                    response = "Count not equal to number of visits"
            elif self.count is None:
                response = "count attribute not found"
            elif self.visits is None:
                response = "visits attribute not found"
            else:
                raise Exception("Unknown Exception in validatorModel.Check_visits")
        except Exception as e:
            response = e

        return response

    def check_visit_attributes(self):
        job_collector = []
        response = False
        if self.count_visit:

            for visit in self.visits:

                try:
                    if "store_id" in visit.keys(): # checking for store ids as when it is missing, the processed image data cant be stored with proper unique id

                        self.cur_store_id = visit["store_id"]
                        if "image_url" in visit.keys():

                            if len(visit["image_url"]) > 0:
                                job_collector.append(visit)  # collecting all jobs here
                            else:
                                response = f"images list empty for store_id: {self.cur_store_id}"
                                break
                        else:
                            response = f"no images listed for store_id: {self.cur_store_id}"
                            break
                    else:
                        response = f"no store_id found right after {self.cur_store_id}"
                        break
                except Exception as e:
                    response = e
            if len(job_collector) == len(self.visits):
                response = True
            return response


def validate_job(json_payload):
    jv_object = JobValidator(json_payload)
    validity = jv_object.check_visits()
    if validity is True:
        print(validity)
        inner_checks = jv_object.check_visit_attributes()
        print(inner_checks)
        return True if inner_checks is True else inner_checks
    else:
        return validity





