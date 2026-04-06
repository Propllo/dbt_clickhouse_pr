{{ config(materialized = "view") }}

with total_crafts as (
    select count(distinct craft) as n
    from {{source("imdb", "people")}}
), 
total_people as (
    select count(distinct name) as n
    from {{source("imdb", "people")}}
)

select
    craft,
    count(distinct name) as n_people_per_cr,
    (select * from total_crafts) as total_crafts,
    (select * from total_people) as total_people,
    arrayStringConcat(arraySort(groupUniqArray(name)), '; ') as list_of_names
from {{source("imdb", "people")}}
group by craft