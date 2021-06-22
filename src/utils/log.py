import logging

# Configure logging to show the name of the thread
# where the log message originates.
logging.basicConfig(
    level=logging.INFO,
    format='%(threadName)10s %(name)18s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../output/parser.log', mode='w+')
    ])


def getLogger(label):
    return logging.getLogger(label)
