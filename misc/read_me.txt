#visualization

--------------------------------------------------------
tables needed: 

explicit songs vs non explicit

keys of songs by year
loudness score and danceability

111) favourite genres 
     fav genres by year

1) count liked songs by year: 
insert into song_count ("year", song_ct)
select 
distinct(right(date_added, 4)) as "year"
,count(track_list) as song_ct
from song_details
group by "year" order by 1 desc;


2) songs by year:
            
select min("2018") as "2018",min("2019") as "2019",min("2020") as "2020",min("2021")as "2021", min("2022") as "2022" from(
select
YEAR_ADDED,
row_number() over(partition by year_added order by track_list) as rn,
case when	YEAR_ADDED = '2018' then track_list else null end as "2018",
case when	YEAR_ADDED = '2019' then track_list else null end as "2019",
case when	YEAR_ADDED = '2020' then track_list else null end as "2020",
case when	YEAR_ADDED = '2021' then track_list else null end as "2021",
case when	YEAR_ADDED = '2022' then track_list else null end as "2022"
from dataeng.year_added group by year_added, track_list) as temp
group by rn order by rn



3) artists by year:

select min("2018") as "2018",min("2019") as "2019",min("2020") as "2020",min("2021")as "2021", min("2022") as "2022" from(
select
YEAR_ADDED,
row_number() over(partition by year_added order by artists_list) as rn,
case when	YEAR_ADDED = '2018' then artists_list else null end as "2018",
case when	YEAR_ADDED = '2019' then artists_list else null end as "2019",
case when	YEAR_ADDED = '2020' then artists_list else null end as "2020",
case when	YEAR_ADDED = '2021' then artists_list else null end as "2021",
case when	YEAR_ADDED = '2022' then artists_list else null end as "2022"
from dataeng.year_added group by year_added, artists_list) as temp
group by rn order by rn

4) avg scores by year:


5) Fav genre by year:


6) top artists


7) top artists by genre


q = "SELECT * FROM df_target LIMIT 3"
sqldf(q, globals())





from pandas_profiling import ProfileReport
prof = ProfileReport(df)
prof.to_file(output_file='report.html')




https://towardsdatascience.com/complete-guide-to-data-visualization-with-python-2dd74df12b5e
df.describe() 
df.info()
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
df.plot(x="artists_list", y="track_list", kind="bar")
#################################################
#################################################
#################################################
#################################################

df_mer.to_csv('C:/projects/Data Engineering/py/apicalls/spotify/data/song_details.csv', encoding='utf-8')


code compartment

connection:



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()




