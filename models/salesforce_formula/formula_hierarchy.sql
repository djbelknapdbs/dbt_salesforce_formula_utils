with formulas as (
  select * from {{ source(var('salesforce_formula_source','salesforce'),'fivetran_formula') }}
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
),

final as (
  select field,
    object,
    sql,
    max(formula_depth) as formula_depth
  from recursive_formulas
  group by 1,2,3
)

select * from final