"""Support module for poc_dbt_history.ipynb.

Everything heavier than a few lines lives here so the notebook stays
readable: build the real Delta / Iceberg tables, vacuum them, mirror
current state into duckdb, drive dbt via subprocess, and probe both
storage-level and row-level (SCD2) history.

The notebook is the narrative; this module is the machinery. All paths
are resolved relative to this file, so the notebook must simply run with
poc/dbt/ as its working directory (which `jupyter nbconvert --execute`
does by default).
"""

from __future__ import annotations

import difflib
import json
import random
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# ---------------------------------------------------------------------------
# Layout & constants

HERE = Path(__file__).resolve().parent
PROJECT = HERE / "project"
DATA = HERE / "data"
IMG = HERE / "img"
LAKEHOUSE = DATA / "lakehouse"
ORDERS_DELTA = LAKEHOUSE / "orders"
CUSTOMERS_DELTA = LAKEHOUSE / "customers"
WAREHOUSE = DATA / "iceberg_warehouse"
DUCKDB_PATH = DATA / "pit_poc.duckdb"
WATERMARKS = DATA / "watermarks.jsonl"
COMPILED = PROJECT / "target" / "compiled" / "pit_poc"
VENV_PY = sys.executable

UTC = timezone.utc
DAY0 = date(2026, 6, 1)
N_DAYS = 20
DAYS = [DAY0 + timedelta(days=i) for i in range(N_DAYS)]
VACUUM_AFTER_DAY = 9  # vacuum the orders Delta table after writing version 9

FOCUS_CUSTOMER = "C03"

CUSTOMER_BASE = [
    ("C01", "Aster Analytics", "smb", "emea"),
    ("C02", "Birch & Co", "consumer", "amer"),
    ("C03", "Cobalt Retail", "trial", "amer"),  # the mutating customer
    ("C04", "Damson Foods", "enterprise", "emea"),
    ("C05", "Elder Logistics", "consumer", "apac"),
    ("C06", "Foxglove Media", "smb", "amer"),
    ("C07", "Gorse Mining", "enterprise", "apac"),
    ("C08", "Hazel Health", "consumer", "emea"),
]

# Cumulative mutations applied at each customer state (1..4). State 1 is the
# base table. C03 walks trial -> consumer -> smb -> enterprise.
STATE_MUTATIONS: dict[int, dict[str, tuple[str, str]]] = {
    2: {"C03": ("segment", "consumer"), "C05": ("segment", "smb")},
    3: {"C03": ("segment", "smb"), "C02": ("region", "apac")},
    4: {"C03": ("segment", "enterprise"), "C06": ("segment", "enterprise")},
}

# Which synthetic days (0-based index into DAYS) each customer state covers,
# for the S6 day-by-day narrative.
STATE_DAY_WINDOWS = {1: range(0, 7), 2: range(7, 13), 3: range(13, 17), 4: range(17, 20)}

RETURN_REASONS = ["damaged", "wrong-item", "changed-mind", "late-delivery"]


# ---------------------------------------------------------------------------
# Workspace

def reset_workspace() -> None:
    """Idempotent start: wipe generated data and dbt build artifacts."""
    for p in (DATA, PROJECT / "target", PROJECT / "logs", HERE / "logs"):
        shutil.rmtree(p, ignore_errors=True)
    for p in (DATA, LAKEHOUSE, WAREHOUSE, IMG):
        p.mkdir(parents=True, exist_ok=True)


def tail(text: str, n: int = 10) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines[-n:])


# ---------------------------------------------------------------------------
# Synthetic business data (deterministic)

def all_orders() -> pd.DataFrame:
    rng = random.Random(42)
    customers = [c[0] for c in CUSTOMER_BASE]
    rows, oid = [], 0
    for d in DAYS:
        for _ in range(rng.randint(3, 8)):
            oid += 1
            rows.append(dict(
                order_id=f"O{oid:04d}",
                customer_id=rng.choice(customers),
                order_date=d,
                amount=round(rng.uniform(20, 400), 2),
            ))
    return pd.DataFrame(rows)


def all_returns(orders: pd.DataFrame) -> pd.DataFrame:
    rng = random.Random(7)
    rows, rid = [], 0
    for o in orders.itertuples():
        if rng.random() < 0.18:
            rid += 1
            rows.append(dict(
                return_id=f"R{rid:04d}",
                order_id=o.order_id,
                return_date=o.order_date + timedelta(days=rng.randint(1, 5)),
                refund_amount=round(o.amount * rng.uniform(0.3, 1.0), 2),
                reason=rng.choice(RETURN_REASONS),
            ))
    return pd.DataFrame(rows)


ORDERS_SCHEMA = pa.schema([
    ("order_id", pa.string()),
    ("customer_id", pa.string()),
    ("order_date", pa.date32()),
    ("amount", pa.float64()),
])

RETURNS_SCHEMA = pa.schema([
    ("return_id", pa.string()),
    ("order_id", pa.string()),
    ("return_date", pa.date32()),
    ("refund_amount", pa.float64()),
    ("reason", pa.string()),
])

CUSTOMERS_SCHEMA = pa.schema([
    ("customer_id", pa.string()),
    ("customer_name", pa.string()),
    ("segment", pa.string()),
    ("region", pa.string()),
])


def _arrow(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pandas(df.reset_index(drop=True), schema=schema,
                                preserve_index=False)


# ---------------------------------------------------------------------------
# Delta: orders (20 day-versions, vacuum mid-history)

def commit_ts(table: Path, version: int) -> datetime:
    """Wall-clock commit timestamp of a Delta version, from the commit log."""
    log_file = table / "_delta_log" / f"{version:020d}.json"
    for line in log_file.read_text().splitlines():
        try:
            action = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "commitInfo" in action:
            ts_ms = action["commitInfo"].get("timestamp", 0)
            return datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    raise RuntimeError(f"no commitInfo in {log_file}")


def build_orders_delta() -> dict:
    """20 overwrite versions = 20 synthetic days; VACUUM after version 9.

    Each version i holds the cumulative orders for days 0..i (a daily batch
    overwrite). retention_hours=0 vacuum after v9 physically deletes the
    data files of v0..v8; versions written afterwards stay readable, so the
    suffix boundary lands mid-history at v9 and all three PIT zones exist.
    """
    orders = all_orders()
    writes: list[dict] = []
    vacuum_info: dict | None = None
    for i, d in enumerate(DAYS):
        batch = orders[orders.order_date <= d]
        write_deltalake(str(ORDERS_DELTA), _arrow(batch, ORDERS_SCHEMA),
                        mode="overwrite")
        v = DeltaTable(str(ORDERS_DELTA)).version()
        writes.append(dict(day_index=i, day=d, version=v, rows=len(batch),
                           committed_at=commit_ts(ORDERS_DELTA, v)))
        time.sleep(1.0)
        if i == VACUUM_AFTER_DAY:
            deleted = DeltaTable(str(ORDERS_DELTA)).vacuum(
                retention_hours=0, dry_run=False,
                enforce_retention_duration=False)
            vacuum_info = dict(after_version=v, files_deleted=len(deleted),
                               vacuum_at=datetime.now(tz=UTC))
    return dict(writes=writes, vacuum=vacuum_info, orders=orders)


def read_version(table: Path, version: int) -> tuple[bool, str]:
    """Empirical probe: really read a Delta version end to end."""
    try:
        t = DeltaTable(str(table), version=version).to_pyarrow_table()
        return True, f"{t.num_rows} rows"
    except Exception as e:  # noqa: BLE001 — the exception IS the evidence
        return False, type(e).__name__


def probe_versions(table: Path) -> pd.DataFrame:
    """Every version the log lists vs. whether it is physically readable."""
    ops = {h["version"]: h.get("operation", "?")
           for h in DeltaTable(str(table)).history()}
    rows = []
    for v in sorted(int(p.stem) for p in (table / "_delta_log").glob("*.json")):
        ok, detail = read_version(table, v)
        rows.append(dict(version=v, operation=ops.get(v, "?"),
                         readable=ok, detail=detail))
    return pd.DataFrame(rows)


def latest_version(table: Path) -> int:
    return DeltaTable(str(table)).version()


def timestamp_pitfall(table: Path, ts: datetime) -> dict:
    """Deliberate demonstration, NOT a bug: timestamp-based time travel
    resolves into vacuumed versions and only fails at read time."""
    dt = DeltaTable(str(table))
    dt.load_as_version(ts)
    resolved = dt.version()
    try:
        t = dt.to_pyarrow_table()
        return dict(resolved_version=resolved, read_ok=True,
                    detail=f"{t.num_rows} rows")
    except Exception as e:  # noqa: BLE001
        return dict(resolved_version=resolved, read_ok=False,
                    error_type=type(e).__name__, error=str(e)[:300])


def storage_read_at(table: Path, ts: datetime) -> dict:
    """Time-travel a Delta table to a wall-clock instant and read it.

    The probe timestamp is capped at the last commit so resolution is
    always defined; failures are physical (files destroyed), not logical.
    """
    last = commit_ts(table, latest_version(table))
    probe_ts = min(ts, last)
    dt = DeltaTable(str(table))
    dt.load_as_version(probe_ts)
    resolved = dt.version()
    try:
        frame = dt.to_pyarrow_table().to_pandas()
        return dict(resolved_version=resolved, read_ok=True, frame=frame)
    except Exception as e:  # noqa: BLE001
        return dict(resolved_version=resolved, read_ok=False,
                    error_type=type(e).__name__)


# ---------------------------------------------------------------------------
# Iceberg: returns (6 ingest batches, out-of-band file destruction)

def get_catalog():
    from pyiceberg.catalog.sql import SqlCatalog
    return SqlCatalog("local", uri=f"sqlite:///{WAREHOUSE}/catalog.db",
                      warehouse=f"file://{WAREHOUSE}")


def build_returns_iceberg(orders: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """6 overwrite batches (cumulative returns feed) with 1s spacing."""
    catalog = get_catalog()
    catalog.create_namespace("raw")
    returns = all_returns(orders)
    tbl = catalog.create_table("raw.returns", schema=RETURNS_SCHEMA)
    cutoffs = [DAY0 + timedelta(days=k) for k in (4, 8, 12, 16, 20, 27)]
    log = []
    for k, cut in enumerate(cutoffs, start=1):
        batch = returns[returns.return_date <= cut]
        tbl.overwrite(_arrow(batch, RETURNS_SCHEMA))
        log.append(dict(batch=k, cutoff=cut, rows=len(batch)))
        time.sleep(1.0)
    return returns, log


def _local_path(file_uri: str) -> str:
    return file_uri[7:] if file_uri.startswith("file://") else file_uri


def destroy_old_iceberg_files(keep_last: int = 2) -> dict:
    """Destroy data files not referenced by the last `keep_last` snapshots —
    out-of-band, exactly like an orphan-file cleaner or aggressive
    expire_snapshots + remove_orphan_files run would."""
    tbl = get_catalog().load_table("raw.returns")
    snaps = sorted(tbl.metadata.snapshots, key=lambda s: s.sequence_number)
    keep: set[str] = set()
    for s in snaps[-keep_last:]:
        for task in tbl.scan(snapshot_id=s.snapshot_id).plan_files():
            keep.add(str(Path(_local_path(task.file.file_path)).resolve()))
    destroyed = []
    for p in WAREHOUSE.rglob("*.parquet"):
        if str(p.resolve()) not in keep:
            p.unlink()
            destroyed.append(p.name)
    summary = [dict(sequence_number=s.sequence_number,
                    snapshot_id=str(s.snapshot_id),
                    operation=(s.summary.operation.value
                               if s.summary else "?"),
                    committed_at=datetime.fromtimestamp(
                        s.timestamp_ms / 1000, tz=UTC))
               for s in snaps]
    return dict(snapshots=pd.DataFrame(summary), kept_files=len(keep),
                destroyed_files=len(destroyed), destroyed=destroyed)


# ---------------------------------------------------------------------------
# duckdb mirror + dbt driver

def duckdb_con(read_only: bool = False):
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def mirror_lake_to_duckdb() -> dict:
    """Copy the CURRENT state of the lake tables into duckdb schema `raw`.

    In production the dbt sources ARE the lake tables (Spark/Trino read
    Delta and Iceberg natively). This PoC executes dbt on duckdb, which has
    no native Delta/Iceberg time travel, so duckdb mirrors the latest state
    — while the WATERMARKS are derived from the real lake tables.
    """
    orders_now = DeltaTable(str(ORDERS_DELTA)).to_pyarrow_table()
    returns_now = get_catalog().load_table("raw.returns").scan().to_arrow()
    con = duckdb_con()
    con.execute("create schema if not exists raw")
    con.register("orders_arrow", orders_now)
    con.register("returns_arrow", returns_now)
    con.execute("create or replace table raw.orders as select * from orders_arrow")
    con.execute("create or replace table raw.returns as select * from returns_arrow")
    con.close()
    return dict(orders_rows=orders_now.num_rows, returns_rows=returns_now.num_rows)


def run_dbt(*args: str, echo_tail: int = 0) -> subprocess.CompletedProcess:
    cmd = [VENV_PY, "-m", "dbt.cli.main", *args, "--profiles-dir", "."]
    proc = subprocess.run(cmd, cwd=str(PROJECT), capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError(f"dbt {' '.join(args)} failed (rc={proc.returncode})")
    if echo_tail:
        print(tail(proc.stdout, echo_tail))
    return proc


def run_alethe_cli(*args: str) -> subprocess.CompletedProcess:
    exe = Path(VENV_PY).parent / "alethe"
    cmd = ([str(exe), *args] if exe.exists()
           else [VENV_PY, "-m", "alethe._cli", *args])
    return subprocess.run(cmd, cwd=str(HERE), capture_output=True, text=True)


def compiled_sql(rel_path: str) -> str:
    return (COMPILED / rel_path).read_text()


def compiled_model_sql(model_name: str) -> str:
    """Compiled SQL for a model, located via the manifest's
    original_file_path (which exactly mirrors target/compiled/<project>/...,
    unlike the schema-relative `path` field)."""
    manifest = json.loads((PROJECT / "target" / "manifest.json").read_text())
    for node in manifest["nodes"].values():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            return (COMPILED / node["original_file_path"]).read_text()
    raise KeyError(f"model {model_name!r} not in manifest")


def unified_diff(a: str, b: str, label_a: str, label_b: str) -> str:
    return "\n".join(difflib.unified_diff(
        a.splitlines(), b.splitlines(), label_a, label_b, lineterm=""))


# ---------------------------------------------------------------------------
# Customers: 4 real mutations -> 4 dbt snapshot runs + a vacuumed Delta mirror

def customers_state(state: int) -> pd.DataFrame:
    df = pd.DataFrame(CUSTOMER_BASE,
                      columns=["customer_id", "customer_name", "segment", "region"])
    for s in range(2, state + 1):
        for cid, (col, val) in STATE_MUTATIONS.get(s, {}).items():
            df.loc[df.customer_id == cid, col] = val
    return df


def run_customers_phase() -> dict:
    """For each of 4 customer states:
      1. overwrite the REAL Delta mirror data/lakehouse/customers,
      2. overwrite duckdb raw.customers (what dbt snapshot reads),
      3. run `dbt snapshot`,
      4. capture a wall-clock probe timestamp inside this state's window.
    After state 2, VACUUM the Delta mirror (retention 0) so state 1's files
    are REALLY destroyed while the SCD2 snapshot keeps every state.
    """
    ts_probe: dict[int, datetime] = {}
    versions: dict[int, int] = {}
    vacuum_info = None
    for s in (1, 2, 3, 4):
        df = customers_state(s)
        write_deltalake(str(CUSTOMERS_DELTA), _arrow(df, CUSTOMERS_SCHEMA),
                        mode="overwrite")
        versions[s] = latest_version(CUSTOMERS_DELTA)
        con = duckdb_con()
        con.execute("create schema if not exists raw")
        con.register("cust_arrow", _arrow(df, CUSTOMERS_SCHEMA))
        con.execute("create or replace table raw.customers as select * from cust_arrow")
        con.close()
        run_dbt("snapshot")
        ts_probe[s] = datetime.now(tz=UTC)
        time.sleep(1.5)
        if s == 2:
            deleted = DeltaTable(str(CUSTOMERS_DELTA)).vacuum(
                retention_hours=0, dry_run=False,
                enforce_retention_duration=False)
            vacuum_info = dict(after_state=2, files_deleted=len(deleted))
    return dict(ts_probe=ts_probe, delta_versions=versions, vacuum=vacuum_info)


def snapshot_relation() -> str:
    """Physical relation of the snapshot table, read from the dbt manifest
    (never hardcoded — dbt's schema resolution owns this name)."""
    manifest = json.loads((PROJECT / "target" / "manifest.json").read_text())
    node = manifest["nodes"]["snapshot.pit_poc.customers_snapshot"]
    rel = node.get("relation_name") or ".".join(
        p for p in (node.get("database"), node.get("schema"), node["name"]) if p)
    return rel.replace('"', "").replace("`", "")


def ensure_utc(value, reference: datetime) -> datetime | None:
    """Normalize a (possibly naive) timestamp from duckdb to aware UTC.

    dbt/duckdb may store snapshot validity columns naive; whether the naive
    value is UTC or local is resolved empirically by picking the reading
    closest to a known wall-clock reference captured during the run.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC").to_pydatetime()
    ref = pd.Timestamp(reference)
    local_tz = datetime.now().astimezone().tzinfo
    candidates = [ts.tz_localize("UTC"), ts.tz_localize(local_tz).tz_convert("UTC")]
    best = min(candidates, key=lambda c: abs((c - ref).total_seconds()))
    return best.to_pydatetime()


def load_snapshot_history(customer_id: str | None = None,
                          reference: datetime | None = None) -> pd.DataFrame:
    rel = snapshot_relation()
    con = duckdb_con(read_only=True)
    q = (f"select customer_id, customer_name, segment, region, "
         f"dbt_valid_from, dbt_valid_to from {rel}")
    if customer_id:
        q += f" where customer_id = '{customer_id}'"
    df = con.execute(q + " order by dbt_valid_from").fetch_df()
    con.close()
    if reference is not None:
        df["valid_from_utc"] = [None if pd.isna(v) else ensure_utc(v, reference)
                                for v in df.dbt_valid_from]
        df["valid_to_utc"] = [None if pd.isna(v) else ensure_utc(v, reference)
                              for v in df.dbt_valid_to]
    return df


def scd2_segment_at(hist: pd.DataFrame, ts: datetime) -> str | None:
    """Row-space PIT lookup over normalized validity windows."""
    for row in hist.itertuples():
        vf, vt = row.valid_from_utc, row.valid_to_utc
        # pandas coerces None back to NaT in datetime columns, so an
        # open-ended window must be detected with isna, not `is None`
        if pd.isna(vf):
            continue
        if vf <= ts and (pd.isna(vt) or ts < vt):
            return row.segment
    return None


def make_snapshot_watermark(hist_all: pd.DataFrame, reference: datetime):
    """Manual OWS watermark for the dbt snapshot chain (design note 1).

    The snapshot is row-space evidence: witnessed at run time (grade
    witnessed-fresh, never `derived`), complete since — and only since —
    the first run. `check` strategy cannot see between-run states, which
    alethe's DbtLineage warns about; that warning is expected.
    """
    from alethe import EvidenceGrade, Watermark
    first = min(ensure_utc(v, reference) for v in hist_all.dbt_valid_from)
    return Watermark(
        chain="snapshot://customers_snapshot",
        boundary={"run": "first"},
        boundary_dt=first,
        earliest_dt=first,
        evidence_grade=EvidenceGrade.WITNESSED_FRESH,
        empirically_validated=True,
        proof={
            "mechanism": "dbt snapshot SCD2 rows (strategy=check, check_cols=all)",
            "rows": int(len(hist_all)),
            "distinct_customers": int(hist_all.customer_id.nunique()),
            "note": ("state witnessed at snapshot-run time; history before "
                     "the first run is unknowable; between-run mutations "
                     "are invisible (check-strategy caveat)"),
        },
    )


def state_of_day(day_index: int) -> int:
    return next(s for s, win in STATE_DAY_WINDOWS.items() if day_index in win)


def s6_probe_table(ts_probe: dict[int, datetime],
                   hist_focus: pd.DataFrame) -> pd.DataFrame:
    """Day-by-day: storage time travel vs SCD2 snapshot for the focus
    customer. Storage loses vacuumed states with a real read error; the
    snapshot resolves every state."""
    rows = []
    for i in range(N_DAYS):
        s = state_of_day(i)
        ts = ts_probe[s]
        probe = storage_read_at(CUSTOMERS_DELTA, ts)
        if probe["read_ok"]:
            frame = probe["frame"]
            seg = frame.loc[frame.customer_id == FOCUS_CUSTOMER, "segment"].iloc[0]
            storage = f"OK  (v{probe['resolved_version']}: segment={seg})"
        else:
            storage = (f"{probe['error_type']}  "
                       f"(resolved v{probe['resolved_version']}, files destroyed)")
        rows.append(dict(
            day=DAYS[i],
            customer_state=s,
            storage_time_travel=storage,
            scd2_snapshot_segment=scd2_segment_at(hist_focus, ts),
        ))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Visuals

def plot_v1(writes: list[dict], vacuum_info: dict, wm_orders, probe: pd.DataFrame):
    """Timeline of the 20 day-versions: readable vs destroyed, vacuum event,
    empirically-validated boundary, and the three PIT zones."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    readable = dict(zip(probe.version, probe.readable))
    xs = [w["committed_at"] for w in writes]
    boundary_x = wm_orders.boundary_dt
    earliest_x = wm_orders.earliest_dt

    fig, ax = plt.subplots(figsize=(13, 4.2))
    pad = timedelta(seconds=3)
    x_lo, x_hi = xs[0] - pad, xs[-1] + pad

    ax.axvspan(x_lo, earliest_x, color="#8e8e8e", alpha=0.25)
    ax.axvspan(earliest_x, boundary_x, color="#e8b339", alpha=0.25)
    ax.axvspan(boundary_x, x_hi, color="#4caf50", alpha=0.15)
    ax.text(earliest_x - pad / 2, 1.38, "UNACHIEVABLE", fontsize=8,
            ha="center", color="#555")
    ax.text(earliest_x + (boundary_x - earliest_x) / 2, 1.38, "BOUNDED",
            fontsize=8, ha="center", color="#8a6d1a")
    ax.text(boundary_x + (x_hi - boundary_x) / 2, 1.38, "CERTAIN",
            fontsize=8, ha="center", color="#2e7d32")

    for w in writes:
        ok = bool(readable.get(w["version"], False))
        if ok:
            ax.plot(w["committed_at"], 1, "o", color="#2e7d32", ms=9, zorder=3)
        else:
            ax.plot(w["committed_at"], 1, "x", color="#c62828", ms=10,
                    mew=2.5, zorder=3)
        ax.annotate(f"v{w['version']}\nday {w['day_index'] + 1}",
                    (w["committed_at"], 1), textcoords="offset points",
                    xytext=(0, -30), ha="center", fontsize=7)

    ax.axvline(boundary_x, color="#1a237e", lw=1.8, ls="--", zorder=2)
    ax.annotate(f"observability boundary\nv{wm_orders.boundary['version']} "
                "(read v9 OK, v8 fails)",
                (boundary_x, 1.22), ha="center", fontsize=8, color="#1a237e")
    vac_x = vacuum_info["vacuum_at"]
    ax.axvline(vac_x, color="#c62828", lw=1.2, ls=":", zorder=2)
    ax.annotate("VACUUM\n(retention 0)", (vac_x, 0.78), ha="center",
                fontsize=8, color="#c62828")

    ax.set_ylim(0.6, 1.5)
    ax.set_yticks([])
    ax.set_xlim(x_lo, x_hi)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.set_xlabel("real commit time (UTC) — one write per synthetic day")
    ax.set_title("delta://orders — the log lists every version; "
                 "VACUUM decided which ones still exist")
    fig.tight_layout()
    fig.savefig(IMG / "v1_orders_timeline.png", dpi=150)
    return fig


def plot_v2(hist_focus: pd.DataFrame, wm_customers, now_ts: datetime):
    """C03's segment over time from SCD2 rows, with the storage boundary and
    the destroyed-evidence region shaded: snapshot coverage extends left of
    what storage can still prove."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    segs = list(dict.fromkeys(hist_focus.segment))
    level = {s: i for i, s in enumerate(segs)}
    fig, ax = plt.subplots(figsize=(13, 3.8))

    first_vf = min(v for v in hist_focus.valid_from_utc if v is not None)
    pad = timedelta(seconds=3)
    x_lo, x_hi = first_vf - pad, now_ts + pad
    boundary_x = wm_customers.boundary_dt

    ax.axvspan(x_lo, boundary_x, color="#c62828", alpha=0.13)
    ax.axvspan(boundary_x, x_hi, color="#4caf50", alpha=0.10)
    ax.axvline(boundary_x, color="#1a237e", lw=1.8, ls="--")
    ax.annotate("storage boundary\n(delta://customers after VACUUM)",
                (boundary_x, len(segs) - 0.45), ha="center", fontsize=8,
                color="#1a237e")
    ax.text(x_lo + (boundary_x - x_lo) / 2, -0.65,
            "storage evidence destroyed\n(time travel -> FileNotFoundError)",
            ha="center", fontsize=8, color="#c62828")
    ax.text(boundary_x + (x_hi - boundary_x) / 2, -0.65,
            "storage recoverable", ha="center", fontsize=8, color="#2e7d32")

    for row in hist_focus.itertuples():
        vf = row.valid_from_utc
        vt = row.valid_to_utc or now_ts
        y = level[row.segment]
        ax.hlines(y, vf, vt, color="#1565c0", lw=4, zorder=3)
        ax.plot(vf, y, "o", color="#1565c0", ms=7, zorder=4)

    ax.set_yticks(range(len(segs)))
    ax.set_yticklabels(segs)
    ax.set_ylim(-1.0, len(segs) - 0.2)
    ax.set_xlim(x_lo, x_hi)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.set_xlabel("real wall-clock time (UTC)")
    ax.set_title(f"{FOCUS_CUSTOMER} segment history — SCD2 snapshot rows "
                 "survive left of the storage boundary")
    fig.tight_layout()
    fig.savefig(IMG / "v2_snapshot_vs_storage.png", dpi=150)
    return fig
