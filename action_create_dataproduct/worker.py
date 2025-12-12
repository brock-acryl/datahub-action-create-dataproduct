import logging
import threading
import time

logger = logging.getLogger(__name__)


def run_worker(stop_event: threading.Event, ctx=None):
    logger.info("Worker started, keeping process alive...")
    
    last_activity_time = time.time()
    
    try:
        while not stop_event.is_set():
            current_time = time.time()
            
            if ctx and current_time - last_activity_time > 20:
                try:
                    source = getattr(ctx, "source", None)
                    if source:
                        if hasattr(source, "_last_event_time"):
                            source._last_event_time = current_time
                        if hasattr(source, "last_event_time"):
                            source.last_event_time = current_time
                        logger.debug("Reset source activity timer to prevent idle timeout")
                    last_activity_time = current_time
                except Exception as e:
                    logger.debug("Could not reset source activity: %s", e)
            
            stop_event.wait(timeout=1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    finally:
        logger.info("Worker stopped")


def start_worker_thread(ctx=None):
    stop_event = threading.Event()
    thread = threading.Thread(target=run_worker, args=(stop_event, ctx), daemon=False)
    thread.start()
    return thread, stop_event


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stop_event = threading.Event()
    run_worker(stop_event)
