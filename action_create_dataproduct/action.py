from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
import logging

logger = logging.getLogger(__name__)


class CreateDataproductAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        logger.info(config_dict)
        return cls(ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def act(self, event: EventEnvelope) -> None:
        logger.info(event)

    def close(self) -> None:
        pass


