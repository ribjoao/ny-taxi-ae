{{ config(materialized='view') }}

select
    {{ dbt_utils.surrogate_key(['dispatching_base_num','PUlocationID']) }} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,

    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    SR_Flag,
    Affiliated_base_number,

from {{ source('staging','materialized_fhv_tripdata') }}
where extract(year from cast(pickup_datetime as timestamp)) = 2019

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}