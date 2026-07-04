{#
  Copyright 2026 Caelan Cooper
  Licensed under the Apache License, Version 2.0.

  alethe — point-in-time source/ref shims for dbt
  ================================================
  Drop this file into your dbt project's macros/ directory. Then:

      dbt run -s revenue_summary --vars '{"alethe_as_of": "2024-03-01"}'

  With `alethe_as_of` unset, compilation is byte-identical to stock dbt.
  With it set:

    - {{ source(...) }}  →  the relation plus engine-native time travel
                            (TIMESTAMP AS OF on Spark/Databricks,
                             FOR TIMESTAMP AS OF on Trino)
    - {{ ref(...) }} of a *snapshot*  →  a validity-window subquery over
                            dbt_valid_from / dbt_valid_to. Snapshots keep
                            history in rows; time-travelling the snapshot
                            table would be a category error.
    - {{ ref(...) }} of a model  →  unchanged. Models are derived; their
                            sources are already bound.

  IMPORTANT: run the alethe PIT report first. This macro binds the query
  but cannot know whether the target time is CERTAIN, BOUNDED, or
  UNACHIEVABLE — binding to a vacuumed point in time will fail at read
  time (Delta/Iceberg) rather than refuse at plan time. Gate CI on:

      lineage.pit_report(model, ...).query(as_of).status
#}

{% macro alethe_as_of_clause() %}
    {%- set as_of = var('alethe_as_of', none) -%}
    {%- if as_of is none -%}
    {%- elif target.type in ('spark', 'databricks') -%}
        {{ return(" TIMESTAMP AS OF '" ~ as_of ~ "'") }}
    {%- elif target.type in ('trino', 'presto', 'athena') -%}
        {{ return(" FOR TIMESTAMP AS OF TIMESTAMP '" ~ as_of ~ "'") }}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "alethe_as_of is set but adapter '" ~ target.type ~
            "' has no time-travel syntax registered in alethe_pit.sql") }}
    {%- endif -%}
{% endmacro %}


{% macro source(source_name, table_name) %}
    {%- set rel = builtins.source(source_name, table_name) -%}
    {%- set as_of = var('alethe_as_of', none) -%}
    {%- if as_of is none -%}
        {{ return(rel) }}
    {%- else -%}
        {{ return(rel ~ alethe_as_of_clause()) }}
    {%- endif -%}
{% endmacro %}


{% macro ref() %}
    {# Pass through all positional/keyword forms of ref() #}
    {%- set rel = builtins.ref(*varargs, **kwargs) -%}
    {%- set as_of = var('alethe_as_of', none) -%}
    {%- if as_of is none -%}
        {{ return(rel) }}
    {%- endif -%}

    {# Look the node up in the graph to see if it is a snapshot #}
    {%- set target_name = varargs[-1] -%}
    {%- set ns = namespace(is_snapshot=false, vf='dbt_valid_from', vt='dbt_valid_to') -%}
    {%- if execute -%}
        {%- for node in graph.nodes.values() -%}
            {%- if node.name == target_name and node.resource_type == 'snapshot' -%}
                {%- set ns.is_snapshot = true -%}
                {# dbt 1.9+ pre-populates these keys with None when unset,
                   so `or` (not a .get default) is required #}
                {%- set meta = node.config.get('snapshot_meta_column_names', {}) or {} -%}
                {%- set ns.vf = meta.get('dbt_valid_from') or 'dbt_valid_from' -%}
                {%- set ns.vt = meta.get('dbt_valid_to') or 'dbt_valid_to' -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if not ns.is_snapshot -%}
        {{ return(rel) }}
    {%- else -%}
        {# Row-space PIT: validity-window subquery, valid in FROM position #}
        {{ return("(select * from " ~ rel
            ~ " where " ~ ns.vf ~ " <= '" ~ var('alethe_as_of') ~ "'"
            ~ " and (" ~ ns.vt ~ " > '" ~ var('alethe_as_of') ~ "'"
            ~ " or " ~ ns.vt ~ " is null))") }}
    {%- endif -%}
{% endmacro %}
