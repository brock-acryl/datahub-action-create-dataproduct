import signal
import sys
import time
import logging

logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    logger.info("Received signal %s, shutting down gracefully", sig)
    sys.exit(0)


def run_worker():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Worker started, keeping process alive...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
        sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_worker()
