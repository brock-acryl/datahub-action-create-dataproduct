import logging
import threading
import time

logger = logging.getLogger(__name__)


def run_worker(stop_event: threading.Event, ctx=None):
    logger.info("Worker started, keeping process alive and resetting idle timer...")
    
    try:
        while not stop_event.is_set():
            try:
                source = getattr(ctx, "source", None) if ctx else None
                if source:
                    current_time = time.time()
                    attrs_to_try = [
                        "_last_event_time",
                        "last_event_time", 
                        "_last_activity_time",
                        "last_activity_time",
                        "_idle_start_time",
                        "idle_start_time"
                    ]
                    for attr in attrs_to_try:
                        if hasattr(source, attr):
                            setattr(source, attr, current_time)
                            logger.debug(f"Reset source.{attr} to prevent idle timeout")
                    
                    if hasattr(source, "kill_after_idle_timeout"):
                        source.kill_after_idle_timeout = False
                    if hasattr(source, "_kill_after_idle_timeout"):
                        source._kill_after_idle_timeout = False
            except Exception as e:
                logger.debug("Could not reset source activity: %s", e)
            
            stop_event.wait(timeout=5)
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
