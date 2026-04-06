{{ config(materialized = "view") }}

select * from {{ source('imdb', 'people') }}