from pyspark.sql.functions import lit, monotonically_increasing_id, row_number
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from photutils.aperture import CircularAperture
from photutils.aperture import CircularAnnulus
from photutils.aperture import ApertureStats
from functools import reduce
import pandas as pd
import numpy as np
import confluent_kafka
import sqlalchemy
import datetime
import logging
import redis
import yaml
import json
import sys
import os


class TessPhotQueue:

    def __init__(self, config_file, debug):
        # Set configuration
        # Parse YAML for config
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)

        # Set logger
        self.logger = logging.getLogger("tesspatrol-queue")
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

        # Set spark session
        self.logger.info("connecting to spark")
        config = SparkConf().setAppName("tesspatrol-spark")
        sc = SparkContext(conf=config, pyFiles=['engine.py', 'app.py'])
        self.spark = SparkSession(sparkContext=sc)

        # Connect to Postgres
        self.logger.info("connecting to postgres")
        url_object = sqlalchemy.URL.create(
                "postgresql+psycopg2",
                username=self.config['db_user'],
                password=self.config['db_pass'],
                database=self.config['db_name'],
                host=self.config['db_host'])
        self.psql_engine = sqlalchemy.create_engine(url_object)

        # Connect to redis
        self.logger.info("connecting to redis")
        self.redis_pool = redis.ConnectionPool()

        # Get Kafka Consumer
        # Create consumer
        kafka_conf = {'bootstrap.servers': f"{self.config['kafka_host']}:9092",
                      'group.id': "tesspatrol",
                      'enable.auto.commit': False,
                      'enable.auto.offset.store': True,
                      'enable.partition.eof': False,
                      'max.poll.interval.ms': 3600000,
                      'session.timeout.ms': 1200000,
                      'heartbeat.interval.ms': 3000,
                      'auto.offset.reset': 'smallest'}
        self.kafka = confluent_kafka.Consumer(kafka_conf)
        self.kafka.subscribe(["tesspatrol_phot"])

    def run_queue(self):
        while True:
            # Get next coadd in Kafka queue
            msg = self.kafka.poll(timeout=1.0)

            # No message, try again
            if msg is None:
                continue

            # Process request in queue
            else:
                lc_req = json.loads(msg.value().decode('utf-8'))
                # Process lightcurve
                self.process_lightcurve(lc_req)

    def process_lightcurve(self, lightcurve_request):
        # State updates
        with redis.Redis(connection_pool=self.redis_pool) as conn:
            conn.hset(lightcurve_request['request_id'], "status", "processing")
            conn.hset("queue_first", str(lightcurve_request['queue_pos']))

            # Read in image info
            img_df = pd.read_parquet(os.path.join(lightcurve_request['data_dir'], "image_meta.parq"))

            # Calc conv_lc
            field_sdf_list = []
            for _, field_group in img_df.groupby('field_id'):
                img_sdf_list = []
                field_meta = {}
                for _, img_meta in field_group.iterrows():
                    # Create dict for field meta
                    field_meta = img_meta.to_dict()

                    # Read data for conv_ and bkg_ images
                    conv_sdf = self.spark.read.format("fits").option("hdu", 0).load(img_meta['conv_path']).drop("ImgIndex")
                    conv_sdf = conv_sdf.withColumnRenamed('Image', 'conv_data')
                    conv_sdf = conv_sdf.withColumn("id", monotonically_increasing_id())
                    conv_sdf = conv_sdf.withColumn('id', row_number().over(Window.orderBy('id')))
                    bkg_sdf = self.spark.read.format("fits").option("hdu", 0).load(img_meta['bkg_path']).drop("ImgIndex")
                    bkg_sdf = bkg_sdf.withColumnRenamed('Image', 'bkg_data')
                    bkg_sdf = bkg_sdf.withColumn("id", monotonically_increasing_id())
                    bkg_sdf = bkg_sdf.withColumn('id', row_number().over(Window.orderBy('id')))
                    # Merge into single dataframe and add meta
                    sdf = conv_sdf.join(bkg_sdf, on='id', how='inner')
                    sdf = sdf.withColumn("img_name", lit(img_meta['img_name']))
                    img_sdf_list.append(sdf)

                # Union all images for field
                field_sdf = reduce(lambda x, y: x.union(y), img_sdf_list)
                # Run apphot for each image
                field_sdf = field_sdf.groupby("img_name").applyInPandas(lambda sdf: TessPhotQueue.spark_apphot(sdf, field_meta))
                field_sdf_list.append(field_sdf)
            # Union all the photometry for img/fields
            phot_sdf = reduce(lambda x, y: x.union(y), field_sdf_list)

            # Write to disk
            phot_df = phot_sdf.toPandas()
            phot_df.to_csv(os.path.join(lightcurve_request['data_dir'], "lightcurve.csv"), index=False)
            phot_df.to_csv(os.path.join(lightcurve_request['data_dir'], "lightcurve.json"), index=False)
            phot_df.to_parquet(os.path.join(lightcurve_request['data_dir'], "lightcurve.parq"), index=False)

            # Report finished
            conn.hset(lightcurve_request['request_id'], "status", "complete")

            # Set info in postgres
            with self.psql_engine.begin() as psql:
                # ALWAYS UTC
                timestamp = datetime.datetime.utcnow().isoformat()
                statement = f"INSERT INTO requests " \
                            f"(request_id, completion_timestamp, status) " \
                            f"VALUES " \
                            f"('{lightcurve_request['request_id']}', '{timestamp}', 'cached')"
                psql.execute(sqlalchemy.text(statement))

    @staticmethod
    def spark_apphot(data_sdf, img_meta):
        # Convert to np arrays
        conv_data = np.vstack(data_sdf["conv_data"])
        bkg_data = np.vstack(data_sdf["bkg_data"])

        # Return phot
        photometry = {
            "img_name": img_meta['img_name'],
            "sector_id": img_meta['sector_id'],
            "camera_id": img_meta['camera_id'],
            "ccd_id": img_meta['ccd_id'],
            "cadence": img_meta['cadence'],
            "mjd_beg": img_meta['mjd_beg'],
            "mjd_mid": img_meta['mjd_mid'],
            "mjd_end": img_meta['mjd_end'],
        }

        # Create aperture and annulus
        src_apt = CircularAperture([img_meta['x'], ['y']], img_meta['aptr'])
        bkg_ann = CircularAnnulus([img_meta['x'], ['y']], img_meta['skyin'], img_meta['skyout'])

        # Get measurements for conv data
        src_phot = ApertureStats(conv_data, src_apt)
        bkg_phot = ApertureStats(conv_data, bkg_ann)
        cts = src_phot.sum - (src_apt.area * bkg_phot.mean)
        cts_per_s = (cts / img_meta['exp_time']) + img_meta['ref_flux']         # ?????
        # Calculate flux error
        e_cts_per_s = np.sqrt(cts_per_s + (src_apt.area * bkg_phot.var) / img_meta['exp_time'])
        # Calculate mag and mag error
        mag = -2.5 * np.log10(np.abs(cts_per_s))
        e_mag = e_cts_per_s / cts_per_s / 0.92
        # Add measurements
        photometry['cts_per_sec'] = cts_per_s
        photometry['e_cts_per_sec'] = e_cts_per_s
        photometry['mag'] = mag
        photometry['e_mag'] = e_mag

        # Get measurements for bkg data
        src_phot = ApertureStats(bkg_data, src_apt)
        bkg_phot = ApertureStats(bkg_data, bkg_ann)
        cts = src_phot.sum - (src_apt.area * bkg_phot.mean)
        cts_per_s = (cts / img_meta['exp_time']) + img_meta['ref_flux']
        # Calculate flux error
        e_cts_per_s = np.sqrt(cts_per_s + (src_apt.area * bkg_phot.var) / img_meta['exp_time'])
        # Calculate mag and mag error
        mag = -2.5 * np.log10(np.abs(cts_per_s))
        e_mag = e_cts_per_s / cts_per_s / 0.92
        # Add measurements
        photometry['bkg'] = 0                # ?????
        photometry['bkg_e_cts_per_sec'] = 0  # ?????
        photometry['bkg_mag'] = 0            # ?????
        photometry['bkg_e_mag'] = 0          # ?????

        # Phot line
        return photometry


def run_queue(config_file, debug):
    phot_queue = TessPhotQueue(config_file, debug)
    phot_queue.run_queue()


