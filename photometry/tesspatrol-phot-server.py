from src.queue import run_queue
from src.server import run_server
from src.app import create_app
import multiprocessing as mp
import argparse

# Init spark context and load libraries
parser = argparse.ArgumentParser()
parser.add_argument("--debug", action="store_true", help="run in debug mode")
parser.add_argument("config_file", type=str, help="configuration file for the api server and engine")
args = parser.parse_args()

# Start photometry queue
q = mp.Process(target=run_queue, args=(args.config_file, args.debug,))
q.start()

# Create application
app = create_app(config_file=args.config_file, debug=args.debug,)

# Run the server
run_server(app)