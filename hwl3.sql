--Оконные функции
SELECT
    userId,
    movieId,
    (rating - MIN(rating) over  (partition by userId))/(MAX(rating) over (partition by userId) - MIN(rating) over  (partition by userId)) as normed_rating,
    avg(rating) over (partition by userId)
from ratings
limit 30;

--"ВАША КОМАНДА СОЗДАНИЯ ТАБЛИЦЫ";
psql -U postgres -c "create table keywords ( id bigint, tags text );"

--"ВАША КОМАНДА ЗАЛИВКИ ДАННЫХ В ТАБЛИЦу";
psql -c "\\copy keywords1 FROM '/usr/local/share/netology_data/raw_data/keywords.csv' DELIMITER ',' CSV HEADER"

--"ЗАПРОС3";
with top_rated as (
select
    movieId
   ,avg(rating) avg_rating
from ratings
group by movieId
having  count(userId) > 50
order by avg_rating desc, movieId asc
limit 150
)
select
    tr.movieId,
    kw.tags
into top_rated_tags
from top_rated tr
inner join keywords1 kw on tr.movieId = kw.id
;

--"ВАША КОМАНДА ВЫГРУЗКИ ТАБЛИЦЫ В ФАЙЛ"
\copy (SELECT * FROM top_rated_tags) TO 'top_rated_tags_file.csv' WITH CSV HEADER DELIMITER as E'\t';
