import json
import logging
import threading
import uuid
from typing import Any, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DataProductUrn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


def _first_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        v = value.strip()
        return v or None
    if isinstance(value, list) and value:
        return _first_str(value[0])
    return None


def _find_first_key(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_first_key(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_first_key(v, key)
            if found is not None:
                return found
    return None


class CreateDataproductAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        return cls(config_dict=config_dict, ctx=ctx)

    def __init__(self, config_dict: dict, ctx: PipelineContext):
        self.config_dict = config_dict
        self.ctx = ctx

        graph = getattr(ctx, "graph", None)
        if graph is None:
            raise ValueError("PipelineContext.graph is required")
        self.graph: DataHubGraph = getattr(graph, "graph", graph)

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

        envelope_raw = event.as_json() if hasattr(event, "as_json") else None
        if not envelope_raw:
            return

        try:
            envelope = json.loads(envelope_raw)
        except Exception:
            logger.exception("Failed to parse event envelope JSON")
            return

        params: dict[str, Any] | None = _find_first_key(envelope, "__parameters_json")
        if not isinstance(params, dict):
            return

        if params.get("actionRequestType") not in (None, "WORKFLOW_FORM_REQUEST"):
            return
        if params.get("operation") not in (None, "COMPLETE"):
            return
        if params.get("result") != "ACCEPTED":
            return

        fields_raw = params.get("fields")
        if not isinstance(fields_raw, str) or not fields_raw.strip():
            return

        try:
            fields = json.loads(fields_raw)
        except Exception:
            logger.exception("Failed to parse approval fields JSON")
            return

        if not isinstance(fields, dict):
            return

        dp_name = _first_str(fields.get("data_product_name"))
        dp_description = _first_str(fields.get("data_product_description"))
        domain_urn = _first_str(fields.get("domain"))

        technical_owner = _first_str(fields.get("technical_owner"))
        business_owner = _first_str(fields.get("business_owner"))

        assets_raw = fields.get("data_assets") or []
        assets: list[str] = [a for a in assets_raw if isinstance(a, str) and a.strip()]

        business_justification = _first_str(fields.get("business_justification"))
        classification = _first_str(fields.get("data_classification"))
        use_cases_raw = fields.get("use_cases") or []
        use_cases: list[str] = [u for u in use_cases_raw if isinstance(u, str) and u.strip()]

        action_request_urn = _find_first_key(envelope, "entityUrn")
        if not isinstance(action_request_urn, str):
            action_request_urn = None

        id_prefix = str(self.config_dict.get("id_prefix", "")).strip()
        action_request_id = None
        if action_request_urn:
            action_request_id = action_request_urn.rsplit(":", 1)[-1].strip() or None
        dp_id = f"{id_prefix}{action_request_id or uuid.uuid4()}"

        dp_urn = DataProductUrn(dp_id).urn()

        actor_urn = params.get("actorUrn") or _find_first_key(envelope, "actor") or "urn:li:corpuser:__datahub_system"
        ts = int(get_sys_time())
        stamp = models.AuditStampClass(time=ts, actor=str(actor_urn))

        custom_properties: dict[str, str] = {}
        if business_justification:
            custom_properties["businessJustification"] = business_justification
        if use_cases:
            custom_properties["useCases"] = json.dumps(use_cases)
        if classification:
            custom_properties["dataClassification"] = classification
        if action_request_urn:
            custom_properties["actionRequestUrn"] = action_request_urn
        workflow_urn = params.get("workflowUrn")
        if isinstance(workflow_urn, str) and workflow_urn.strip():
            custom_properties["workflowUrn"] = workflow_urn

        owners: list[models.OwnerClass] = []
        if technical_owner:
            owners.append(
                models.OwnerClass(
                    owner=technical_owner, type=models.OwnershipTypeClass.TECHNICAL_OWNER
                )
            )
        if business_owner and business_owner != technical_owner:
            owners.append(
                models.OwnerClass(
                    owner=business_owner, type=models.OwnershipTypeClass.BUSINESS_OWNER
                )
            )

        asset_associations = [
            models.DataProductAssociationClass(
                destinationUrn=asset_urn,
                created=stamp,
                lastModified=stamp,
                properties={"source": "workflow_form_request"},
            )
            for asset_urn in assets
        ]

        aspects: list[Any] = [
            models.DataProductKeyClass(id=dp_id),
            models.DataProductPropertiesClass(
                name=dp_name,
                description=dp_description,
                customProperties=custom_properties or None,
                assets=asset_associations or None,
            ),
            models.StatusClass(removed=False),
        ]

        if domain_urn:
            aspects.append(models.DomainsClass(domains=[domain_urn]))
        if owners:
            aspects.append(models.OwnershipClass(owners=owners, lastModified=stamp))

        for aspect in aspects:
            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(entityUrn=dp_urn, aspect=aspect),
                async_flag=False,
            )

        logger.info("Created/updated data product %s (%s)", dp_name or dp_id, dp_urn)

    def close(self) -> None:
        self._worker_shutdown.set()
        self._worker_thread.join(timeout=10.0)
        super().close()


