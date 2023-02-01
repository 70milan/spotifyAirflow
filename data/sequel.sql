drop view if exists master_sp.tracks_yearwise;
create view master_sp.tracks_yearwise as 
select min("2018") as "2018",min("2019") as "2019",min("2020") as "2020",min("2021")as "2021", min("2022") as "2022", min("2023") as "2023" from(
select
date_added,
row_number() over(partition by left(date_added, 4) order by track_list) as rn,
case when	left(date_added, 4) = '2018' then track_list else null end as "2018",
case when	left(date_added, 4) = '2019' then track_list else null end as "2019",
case when	left(date_added, 4) = '2020' then track_list else null end as "2020",
case when	left(date_added, 4) = '2021' then track_list else null end as "2021",
case when	left(date_added, 4) = '2022' then track_list else null end as "2022",
case when	left(date_added, 4) = '2023' then track_list else null end as "2023"
from master_sp.dim_details_1 group by date_added, track_list) as temp
group by rn order by rn;

/* artists*/
drop view if exists master_sp.artists_yearwise;
create view master_sp.artists_yearwise as 
select min("2018") as "2018",min("2019") as "2019",min("2020") as "2020",min("2021")as "2021", min("2022") as "2022",min("2023") as "2023" from(
select
date_added,
row_number() over(partition by left(date_added, 4) order by artists_list) as rn,
case when	left(date_added, 4) = '2018' then artists_list else null end as "2018",
case when	left(date_added, 4) = '2019' then artists_list else null end as "2019",
case when	left(date_added, 4) = '2020' then artists_list else null end as "2020",
case when	left(date_added, 4) = '2021' then artists_list else null end as "2021",
case when	left(date_added, 4) = '2022' then artists_list else null end as "2022",
case when	left(date_added, 4) = '2023' then artists_list else null end as "2023"
from master_sp.dim_details_1 group by date_added, artists_list) as temp
group by rn order by rn;


/*As soon as the program runs, 
these queries should execute 
in master_sp schema to create views

count liked songs by year 


drop view if exists master_sp.song_count;
create view master_sp.song_count as
select 
distinct(left(date_added, 4)) as "year"
,count(track_list) as song_ct
from master_sp.dim_details_1
group by "year" order by 1 desc;

*/




