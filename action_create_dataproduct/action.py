from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class CreateDataproductAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        print(config_dict)
        return cls(ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def act(self, event: EventEnvelope) -> None:
        print(event)

    def close(self) -> None:
        pass


