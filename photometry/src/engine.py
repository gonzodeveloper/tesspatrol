import json

from photutils.aperture import CircularAperture
from photutils.aperture import CircularAnnulus
from photutils.aperture import ApertureStats
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS
from astropy.io import fits
import astropy.units as u
import multiprocessing as mp
import numpy as np
import pandas as pd
import confluent_kafka
import sqlalchemy
import itertools
import redis
import socket
import logging
import yaml
import sys
import os


class TessPhotEngine:

    def __init__(self, config_file, debug):
        # Parse YAML for config
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)

        # Set logger
        self.logger = logging.getLogger("tesspatrol-engine")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        log_file = self.config['log_file']
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        # Connect to Postgres
        self.logger.info("connecting to postgres")
        url_object = sqlalchemy.URL.create(
                "postgresql+psycopg2",
                username=self.config['db_user'],
                password=self.config['db_pass'],
                database=self.config['db_name'],
                host=self.config['db_host'])
        self.psql_engine = sqlalchemy.create_engine(url_object)

        # Get field and sector information
        self.logger.info("loading sector/field information")
        with self.psql_engine.begin() as conn:
            statement = "SELECT * FROM fields JOIN sectors USING (sector_id)"
            self.field_df = pd.read_sql(statement, conn)

        # Read in wcs and calibration information
        self.field_df['wcs'] = self.field_df.apply(
            lambda row: WCS(fits.getheader(row['field_path'] + "ref.wcs")), axis=1)
        # Calibration string prefix (for polynomial correction
        prefix = f"A{self.config['phot_aptr']}O{self.config['calibration_order']}i{self.config['phot_skyin']}o{self.config['phot_skyout']}"
        self.field_df['cal_polynomial'] = self.field_df.apply(
            lambda row: np.array(
                [fits.getheader(row['field_path'] + "ref.wcs")[f"{prefix}X{i}Y{j}"]
                 for i, j in itertools.product(range(self.config['calibration_order'] + 1), range(self.config['calibration_order'] + 1))])
            , axis=1)

        # Connect to redis
        self.logger.info("connecting to redis")
        self.redis_pool = redis.ConnectionPool()
        # Set redis queue number
        with redis.Redis(connection_pool=self.redis_pool) as conn:
            conn.set("queue_first", str(0))
            conn.set("queue_last", str(0))

        # Connect to kafka producer
        conf = {'bootstrap.servers': 'localhost:9092',
                # 'delivery.timeout.ms': 1000,
                'queue.buffering.max.messages': 1000,
                'queue.buffering.max.ms': 5000,
                'batch.num.messages': 16,
                'client.id': socket.gethostname()}
        self.kafka_producer = confluent_kafka.Producer(conf)

    def validate_token(self, token):
        return token

    def submit_phot_request(self, lightcurve_request):
        with redis.Redis(connection_pool=self.redis_pool) as conn:
            # Make request directory
            request_dir = os.path.join(self.config['request_data_dir'], lightcurve_request['request_id'])
            os.makedirs(request_dir, exist_ok=True)

            # State updates
            conn.hset(lightcurve_request['request_id'], "status", "constructing")
            conn.hset(lightcurve_request['request_id'], "data_dir")

            # Get field information for request
            valid_fields = self.calculate_field_pixels(lightcurve_request['ra'],
                                                       lightcurve_request['dec'],
                                                       lightcurve_request['mjd_min'],
                                                       lightcurve_request['mjd_max'])

            # Only run with reference flux if requested
            if lightcurve_request['ref_flux_correction']:
                # Get reference flux for all fields
                with mp.Pool(processes=16) as pool:
                    results = [pool.apply_async(self.ref_flux, args=(field['field_path'] + "ref.fits",
                                                                     field['coords'][0],
                                                                     field['coords'][1],
                                                                     field['exp_time'],
                                                                     field['cal_polynomial']))
                               for _, field in valid_fields.iterrows()]
                    valid_fields['ref_flux'] = [r.get() for r in results]
            else:
                valid_fields['ref_flux'] = 0

            # Get all images in fields
            image_df = self.get_images(valid_fields, lightcurve_request['mjd_min'], lightcurve_request['mjd_max'])

            # Store to request data dir and submit to processing queue
            image_df.to_parquet(os.path.join(lightcurve_request['data_dir'], "image_meta.parq"))

            # Get queue position for monitoring
            queue_pos = int(conn.hget("queue_last"))

            # Submit
            self.kafka_producer.produce(topic='tesspatrol',
                                        value=json.dumps({
                                            'request_id': lightcurve_request['request_id'],
                                            'data_dir': request_dir,
                                            'queue_pos': queue_pos
                                        }).encode('utf-8'),
                                        callback=self.acked)
            self.kafka_producer.poll(timeout=1.0)

            # Update status
            conn.hset("queue_last", str(queue_pos + 1))
            conn.hset(lightcurve_request['request_id'], "queue_position", str(queue_pos))
            conn.hset(lightcurve_request['request_id'], "status", "queued")
            conn.hset(lightcurve_request['request_id'], "n_images", str(len(image_df)))

            return {"request_id": lightcurve_request, "status": "queued"}

    def calculate_field_pixels(self, ra, dec, mjd_min, mjd_max):
        # First reduce to sectors with given dates
        valid_fields = self.field_df[(self.field_df['mjd_start'] < mjd_max) & (self.field_df['mjd_finish'] > mjd_min)]
        # Create Skycoord
        coord = SkyCoord(ra, dec, unit=(u.hourangle, u.degree), frame='fk5')
        # Check wcs
        with mp.Pool(processes=16) as pool:
            results = [pool.apply_async(wcs.contains, args=(coord, )) for wcs in valid_fields['wcs'].to_list()]
            valid_fields['coinaints_coord'] = [r.get() for r in results]
            # Now reduce to fields contianing ra and dec
            valid_fields = valid_fields[valid_fields['contains_coord']]
            # Finally, get x and y values for valid fields
            results = [pool.apply_async(wcs.world_to_pixel, args=(coord, )) for wcs in valid_fields['wcs'].to_list()]
            # Append to dataframe
            valid_fields['coords'] = [r.get() for r in results]

        return valid_fields

    def get_images(self, field_df, mjd_min, mjd_max):
        # Connect to postgres and get all valid images
        with self.psql_engine.begin() as conn:
            field_img_dfs = []
            for _, field in field_df.iterrows():
                statement = f"SELECT " \
                            f"  field_id," \
                            f"  sector_id,"   \
                            f"  camera_id, "  \
                            f"  ccd_id, "     \
                            f"  cadence, "    \
                            f"  exp_time, " \
                            f"  mjd_beg, " \
                            f"  mjd_mid, " \
                            f"  mjd_end, " \
                            f"  filename AS img_name,  " \
                            f"  CONCAT(field_path, 'conv_', filename) AS conv_path " \
                            f"  CONCAT(field_path, 'bkg_', filename) AS bkg_path " \
                            f"  {field['coords'][0]} AS x, " \
                            f"  {field['coords'][1]} AS y, " \
                            f"  {self.config['phot_aptr']} AS aptr, " \
                            f"  {self.config['phot_skyin']} AS skyin, " \
                            f"  {self.config['phot_skyout']} AS skyout, " \
                            f"  {field['ref_flux']} AS ref_flux " \
                            f"FROM sectors " \
                            f"JOIN fields USING (sector_id) " \
                            f"JOIN images USING (field_id) " \
                            f"WHERE 1=1 " \
                            f"  AND field_id = {field['field_id']} " \
                            f"  AND mjd_beg < {mjd_max} " \
                            f"  AND mjd_end > {mjd_min}"
                df = pd.read_sql(statement, df)
                field_img_dfs.append(df)
            return pd.concat(field_img_dfs)

    def ref_flux(self, ref_path, x, y, exp_time, cal_polynomial):
        # Read in fits get header
        with fits.open(ref_path) as hdul:
            hdr = hdul[0].header
            data = hdul[0].data
        # Get fractional x/y pos
        xpos = x / hdr['NAXIS1']
        ypos = y / hdr['NAXIS2']
        # Calc polynomial correction as z
        ij = itertools.product(range(self.config['calibration_order'] + 1), range(self.config['calibration_order'] + 1))
        z = np.zeros_like(xpos)
        for a, (i, j) in zip(cal_polynomial, ij):
            z += a * xpos ** i * ypos ** j

        # Run aperture photometry using phot_utils
        src_app = CircularAperture([x, y], self.config['phot_aptr'])
        bkg_ann = CircularAnnulus([x, y], self.config['phot_skyin'], self.config['phot_skyout'])
        src_phot = ApertureStats(data, src_app)
        bkg_phot = ApertureStats(data, bkg_ann)
        cts = src_phot.sum - (src_app.area * bkg_phot.mean)
        cts_per_s = cts / exp_time
        return cts_per_s

    def get_request_status(self, request_id):
        with redis.Redis(connection_pool=self.redis_pool) as conn:
            # Get all information for lightcurve request
            request_status = conn.hgetall(request_id)
            # Nothing in redis, check postgres
            if request_status is None:
                with self.psql_engine.begin() as psql:
                    res_df = pd.read_sql(f"SELECT * FROM requests WHERE request_id = '{request_id}'", psql)
                    # Nothing in postgres, return None
                    if len(res_df) == 0:
                        response = None
                    else:
                        response = res_df[0].to_dict()
            else:
                if "queue_pos" in request_status.keys():
                    # Determine queue position for
                    queue_first = int(conn.get("queue_first"))
                    queue_pos = int(request_status['queue_pos'])
                    request_status['queue_position'] = queue_pos - queue_first
                    response = request_status
                    # Delete irrelevant columns
                    del request_status['queue_pos']
                # User doesnt need
                del request_status['data_dir']

            return response

    def get_lightcurve(self, request_id, format):
        # Check postgres status
        with self.psql_engine.begin() as psql:
            res_df = pd.read_sql(f"SELECT * FROM requests WHERE request_id = '{request_id}'", psql)
            # Bad request id
            if len(res_df) == 0:
                response =  None
            else:
                # Process
                req_info = res_df[0].to_dict()
                if req_info['status'] == 'expired':
                    response = "expired"
                else:
                    response = os.path.join(self.config['request_data_dir'], request_id, f"lightcurve.{format}")

        return response

    def acked(self, err, msg):
        if err is not None:
            msg = msg.value().decode('utf-8')
            msg = json.loads(msg)
            self.logger.error(msg)





