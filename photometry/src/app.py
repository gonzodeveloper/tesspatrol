from .engine import TessPhotEngine
from flask import Blueprint, Flask, request, make_response, send_file
from flask_cors import cross_origin
import logging
import yaml
import json
import sys


# Init app main
main = Blueprint('main', __name__)


def create_app(config_file, debug):
    global tess_engine
    global logger

    # Parse YAML for config
    with open(config_file) as stream:
        config = yaml.safe_load(stream)

    # Set logger
    logger = logging.getLogger("tesspatrol-api")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    log_file = config['log_file']
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Start TESS Patrol Photometry Engine
    tess_engine = TessPhotEngine(config_file, debug)

    # Start Application
    app = Flask(__name__)
    app.register_blueprint(main)

    return app


@main.route("/submit_lightcurve_request/", methods=['POST'])
@cross_origin
def submit_lightcurve_request():
    # Metadata
    request_json = request.get_json()
    log = request_json
    log['query_ip'] = request.remote_addr

    # Only get lightcurve if token is valid
    if tess_engine.validate_token(request_json['token']):
        response = tess_engine.submit_phot_request(request_json['lightcurve_request'])
        code = 200
    else:
        response = {"error_text": "invalid token"}
        code = 401

    return application_response(response, code)


@main.route("/check_request_status/request_id_<string:request_id>")
@cross_origin
def check_request_status(request_id):
    # Metadata
    request_json = request.get_json()
    log = request_json
    log['query_ip'] = request.remote_addr

    # Engine checks redis and postgres for request information
    response = tess_engine.get_request_status(request_id)
    code = 200

    # Bad request_id
    if response is None:
        response = {"error_text": "invalid request_id"}
        code = 400

    return application_response(response, code)


@main.route("/download_lightcurve/request_id_<string:request_id>_format_<string:format>")
@cross_origin()
def download_lightcurve(request_id, format):
    # Metadata
    request_json = request.get_json()
    log = request_json
    log['query_ip'] = request.remote_addr

    # Engine get file
    response = tess_engine.get_lightcurve(request_id, format)
    # Check faults
    if response is None:
        response = {"error_text": "invalid request_id"}
        code = 400
        application_response(response, code)
    elif response == "expired":
        response = {"error_text": "expired lightcurve data"}
        code = 404
        application_response(response, code)
    else:
        return send_file(response, as_attachment=True)


def application_response(response, code):
    response = make_response(json.dumps(response))
    response.mimetype = 'application/json'
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.status_code = code
    return response