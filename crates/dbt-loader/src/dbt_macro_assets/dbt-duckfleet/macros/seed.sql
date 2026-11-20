{# DuckFleet overrides DuckDB's seed loader. DuckFleet runs on a REMOTE gateway:
   it cannot COPY from a local path, and it has no local-file Arrow bulk ingest
   (DuckDB's `fast` path sets `allowed_directories` and reads the file server-side,
   neither of which the gateway can do). So load seeds as parameter-bound INSERTs
   of the already-parsed client-side rows — `adapter.add_query` inlines the bindings
   into the SQL, so no server-side file access is ever required. CSV paths belong to
   the dbt client; we send values, never paths.

   This is duckdb__load_csv_rows's non-`fast` branch verbatim; the helper macros
   (get_batch_size, get_binding_char, get_seed_column_quoted_csv) resolve through
   dispatch to their duckdb__/default__ definitions. #}
{% macro duckfleet__load_csv_rows(model, agate_table) %}
    {% set batch_size = get_batch_size() %}
    {% set agate_table = adapter.convert_datetimes_to_strs(agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    {{ get_binding_char() }}
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}
