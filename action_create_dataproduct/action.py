from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
import logging
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CreateDataproductAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        logger.info(config_dict)
        return cls(config_dict=config_dict, ctx=ctx)

    def __init__(self, config_dict: dict, ctx: PipelineContext):
        self.config_dict = config_dict
        self.ctx = ctx
        self._worker_shutdown = threading.Event()
        self._worker_exc: Optional[BaseException] = None

        worker_name = str(config_dict.get("worker_name", "dataproduct-worker"))
        keep_process_alive = bool(config_dict.get("keep_process_alive", True))

        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            args=(self._worker_shutdown,),
            daemon=not keep_process_alive,
            name=worker_name,
        )
        self._worker_thread.start()

    def _worker_loop(self, shutdown_event: threading.Event) -> None:
        interval_s_raw: Any = self.config_dict.get("worker_interval_seconds", 60.0)
        try:
            interval_s = float(interval_s_raw)
        except Exception:
            interval_s = 60.0
        interval_s = max(0.1, interval_s)

        try:
            while not shutdown_event.is_set():
                logger.debug("Background worker heartbeat (interval_s=%s)", interval_s)
                if shutdown_event.wait(timeout=interval_s):
                    break
        except BaseException as e:
            self._worker_exc = e
            logger.exception("Background worker crashed")

    def act(self, event: EventEnvelope) -> None:
        if self._worker_exc is not None:
            raise SystemExit("Background worker crashed") from self._worker_exc
        if not self._worker_thread.is_alive():
            raise SystemExit("Background worker thread died")
        logger.info(event)

    def close(self) -> None:
        self._worker_shutdown.set()
        self._worker_thread.join(timeout=10.0)
        super().close()


