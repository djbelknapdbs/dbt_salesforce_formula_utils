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
            where object='{{ join_to_table }}'
          ), 

          field_list as (
              select distinct lower(column_name) as field
              from {{ fivetran_formula.database }}.information_schema.columns
              where lower(table_name)=lower('{{ join_to_table }}')
            
              union
            
              select distinct field
              from formulas
          ),

          clean_formulas as (
              select field,
                  case 
                      when sql is null then 'null_value'
                      when sql like '\'%\'' then sql --Hard-coded value
                      when sql like '%.%' then 'null_value' --Salesforce system variables
                      else sql --References to schema.table aren't valid, need to be database.schema.table
                  end as clean_sql
              from formulas
          ),

          base_formulas as (
              select distinct
                  cf.field,
                  cf.clean_sql,
                  case when cf.clean_sql='null_value' then false
                      when cf.clean_sql like '\'%\'' then false
                      when s.field is null then false
                      else true
                  end as references_formula
              from clean_formulas as cf
                  left join clean_formulas s on regexp_like(cf.clean_sql, ('.*\\b' || s.field || '\\b.*'), 'i') 
          ),

          recursive_formulas as (
            select field,
              clean_sql,
              1 as formula_depth
            from base_formulas
            where references_formula=false
            
            union all
            
            select base_formulas.field, 
              base_formulas.clean_sql, 
              formula_depth+1 as formula_depth
            from base_formulas
              inner join recursive_formulas on regexp_like(base_formulas.clean_sql, ('.*\\b' || recursive_formulas.field || '\\b.*'), 'i')
          ),

          field_references as (
            select recursive_formulas.field,
              recursive_formulas.clean_sql,
              max(recursive_formulas.formula_depth) as formula_depth,
              array_agg(field_list.field) as references_fields
            from recursive_formulas
                left join field_list on regexp_like(recursive_formulas.clean_sql, ('.*\\b' || field_list.field || '\\b.*'), 'i')
            group by 1,2
          )

          select
              field as key,
              clean_sql as value
          from field_references
          where value='null_value' 
              or value like '\'%\''
              or array_size(references_fields)>0
          order by formula_depth

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