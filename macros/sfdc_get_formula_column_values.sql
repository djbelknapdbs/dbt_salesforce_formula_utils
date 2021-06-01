{% macro sfdc_get_formula_column_values(fivetran_formula, key, value, join_to_table, max_records=none, default=none) -%}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}
{#--  #}

    {%- set target_relation = adapter.get_relation(database=fivetran_formula.database,
                                          schema=fivetran_formula.schema,
                                         identifier=fivetran_formula.identifier) -%}

    {%- call statement('get_column_values', fetch_result=true) %}

        {%- if not target_relation and default is none -%}

          {{ exceptions.raise_compiler_error("In get_column_values(): relation " ~ fivetran_formula ~ " does not exist and no default value was provided.") }}

        {%- elif not target_relation and default is not none -%}

          {{ log("Relation " ~ fivetran_formula ~ " does not exist. Returning the default value: " ~ default) }}

          {{ return(default) }}

        {%- else -%}

          with formulas as (
            select * 
            from {{ target_relation }}
            where object = '{{ join_to_table }}'
          ), 

          base_formulas as (
            select f.* 
            from formulas f
              left join formulas l on f.sql regexp '.*\\b' || l.field || '\\b.*'
            where l.field is null
          ),

          recursive_formulas as (
            select field, 
              object,
              sql,
              1 as formula_depth
            from base_formulas
            
            union all
            
            select formulas.field, 
              formulas.object,
              formulas.sql, 
              formula_depth+1 as formula_depth
            from formulas
              inner join recursive_formulas on formulas.sql regexp ('.*\\b' || recursive_formulas.field || '\\b.*')
          )
          
          select
              {{ key }} as key,
              case when {{ value }} is null or {{ value }} like '%$%.%'
                  then 'null_value'
                  else {{ value }}
                      end as value

          from recursive_formulas
          group by 1, 2
          order by max(formula_depth)

          {% if max_records is not none %}
          limit {{ max_records }}
          {% endif %}

        {% endif %}

    {%- endcall -%}

    {%- set value_list = load_result('get_column_values') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | list %}
        {{ return(values) }}
    {%- else -%}
        {{ return(default) }}
    {%- endif -%}

{%- endmacro %}