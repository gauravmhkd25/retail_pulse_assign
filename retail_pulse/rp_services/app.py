from flask import Flask, request, make_response, jsonify
import datetime
from rp_services.validatorModel import validate_job
from rp_services.schedular import RegisterJob
import logging
import time

app = Flask(__name__)
DEBUG = True
app.config['JSON_SORT_KEYS'] = False
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)
current_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


@app.route('/api/health_check')
def index():
    register_object = RegisterJob()
    db_health = register_object.check_db_health()
    if db_health is True:
        response = make_response(jsonify({"message":{"db connection":db_health,"schedular": True},"status":"OK"}), 200)
    else:
        response = make_response(jsonify({"message": {"db connection": db_health, "schedular": True},"status":"Server has internal error"}), 500)
    return response


@app.route('/api/submit', methods=['POST'])
def submit():
    register_object = RegisterJob()
    try:
        jobs_payload = request.json
        print(jobs_payload)
        jv_object = validate_job(jobs_payload)
        logging.info("/upload request entry by unique requester Id: ")
        logging.info("{} requested with file , requesting: ")
        if jv_object is True:
            save_status,job_id = register_object.save_job(jobs_payload)
            if save_status is True:
                response, status_code = {"message": "job accepted", "jobID":job_id}, 201
            else:
                response, status_code = {"message": "job declined", "error": save_status}, 400
        else:
            response, status_code = {"message": "job declined", "error": jv_object}, 400

    except Exception as e:
        response, status_code = {"message": "job declined", "error": e}, 501

    return make_response(jsonify(response), status_code)


@app.route('/api/status', methods=['get'])
def get_request_status():
    register_object = RegisterJob()
    try:
        job_id = int(request.args['jobid'])
        print(job_id)
        res, status_code = register_object.get_job_status(job_id)
        print(res)
        return make_response(jsonify(res), status_code)
    except Exception as e:
        return make_response(jsonify({"status": "failed", "error": f"could not fetch from DB - {e}"}),500)


@app.route('/api/visits', methods=['get'])
def get_visit_info_within_dates():
    register_object = RegisterJob()
    area = int(request.args['area'])
    storeid = request.args['storeid']
    start_date = request.args['startDate']
    end_date = request.args['endDate']
    print(area,storeid,start_date,end_date)
    logging.info(f"Received visit info request for Area: {area}, Between {start_date} and {end_date}")
    try:
        start_ds = time.strptime(start_date, "%Y-%m-%d")
        start_epoch = round(time.mktime(start_ds)*1000)
        end_ds = time.strptime(end_date, "%Y-%m-%d")
        end_epoch = round(time.mktime(end_ds)*1000)
        print(start_epoch,end_epoch)
    except Exception as e:
        return make_response(jsonify({"status": "failed", "error": f"Date entry wrong - {e}"}),400)

    try:
        res, status_code = register_object.get_visit_info(storeid,area,start_epoch,end_epoch)
        print(res)
        response = make_response(jsonify(res), status_code)
    except Exception as e:
        response = make_response(jsonify({"status": "failed", "error": f"could not fetch from DB {e}"}),500)

    return response


if __name__ == '__main__':
    app.run()

