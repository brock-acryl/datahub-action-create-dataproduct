import logging
import threading
import time

logger = logging.getLogger(__name__)


def run_worker():
    logger.info("Worker started, keeping process alive...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")


def start_worker_thread():
    thread = threading.Thread(target=run_worker, daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_worker()
