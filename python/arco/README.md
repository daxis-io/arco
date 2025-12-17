# Arco Servo Python SDK

Python SDK for defining data assets and orchestrating pipelines with Arco Servo.

## Installation

```bash
pip install arco-servo
```

## Quick Start

```python
from servo import asset, AssetIn, AssetContext
from servo.types import DailyPartition

@asset(
    description="Daily user metrics",
    partitions=DailyPartition("date"),
)
def user_metrics(
    ctx: AssetContext,
    raw_events: AssetIn["raw_events"],
) -> None:
    events = raw_events.read()
    ctx.output(events.group_by("user_id").agg(...))
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy src/

# Linting
ruff check src/ tests/
```
