import logging
import threading
import time

logger = logging.getLogger(__name__)


def run_worker(stop_event: threading.Event):
    logger.info("Worker started, keeping process alive...")
    
    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    finally:
        logger.info("Worker stopped")


def start_worker_thread():
    stop_event = threading.Event()
    thread = threading.Thread(target=run_worker, args=(stop_event,), daemon=False)
    thread.start()
    return thread, stop_event


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stop_event = threading.Event()
    run_worker(stop_event)
