# datahub-action-create-dataproduct

Creates/updates a DataHub **Data Product** when an approval event is received for a workflow form request (`result=ACCEPTED`).

### What it does
- **Creates/updates a Data Product** (id derived from the `actionRequest` UUID).
- **Sets**: name, description, domain, ownership (technical + business), and `Status(removed=false)`.
- **Links assets** provided by the form to the data product.
- Stores some form fields into **custom properties**: `businessJustification`, `useCases`, `dataClassification`, `workflowUrn`, `actionRequestUrn`.

## Install

```bash
python -m pip install -e .
```

## Run

```bash
export DATAHUB_TOKEN="<your-datahub-token>"
datahub actions -c action.yaml
```

### Config
Add these under `action.config`:

- **`id_prefix`** (optional): prefix for the generated data product id (e.g. `dp-`).
- **`keep_process_alive`** (default: `true`): keep the action process alive.
- **`worker_name`** (default: `dataproduct-worker`): thread name for the heartbeat worker.
- **`worker_interval_seconds`** (default: `60`): heartbeat interval.

Example:

```yaml
action:
  type: "action-create-dataproduct"
  config:
    id_prefix: "dp-"
    keep_process_alive: true
    worker_interval_seconds: 60
```

