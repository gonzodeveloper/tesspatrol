from paste.translogger import TransLogger
import cherrypy
import yaml


def run_server(app, config_file):

    # Parse YAML for config
    with open(config_file) as stream:
        config = yaml.safe_load(stream)

    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': config['host_port'],
        'server.socket_host': config['host_address']
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
