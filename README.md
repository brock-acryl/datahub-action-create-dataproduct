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

To prevent the pipeline from exiting when no events arrive, set this under `source.config`:

- **`kill_after_idle_timeout`** (default: `true`): set to `false` to disable the 30s idle shutdown in the DataHub events source.

Example:

```yaml
source:
  type: "datahub-cloud"
  config:
    kill_after_idle_timeout: false
action:
  type: "action-create-dataproduct"
  config:
    id_prefix: "dp-"
```

