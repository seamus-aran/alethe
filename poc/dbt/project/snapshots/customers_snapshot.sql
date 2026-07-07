{% snapshot customers_snapshot %}
{{ config(
    unique_key='customer_id',
    strategy='check',
    check_cols='all',
    schema='snapshots'
) }}
-- Each `dbt snapshot` run diffs raw.customers against the snapshot table
-- and closes/opens SCD2 validity windows (dbt_valid_from / dbt_valid_to).
-- History accumulates in ROWS: it survives any storage-level VACUUM.
--
-- Honest caveat (alethe warns about this, and the warning is expected):
-- strategy='check' witnesses state at snapshot-run time only. It cannot
-- reconstruct states between runs, and history before the first run is
-- unknowable — hence the snapshot chain's watermark is graded
-- witnessed-fresh with boundary = first run, not `derived`.
select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
