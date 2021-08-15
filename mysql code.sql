/*Creating database Streaming_Services*/
create database Streaming_Services;

/*Connecting with the database created*/
use Streaming_Services;

/*This database contains two tables namely movies and otts in which ott was imported using "Table data import wizard" (because small table) and movies by using cmd (because large data) */

/*Defining the table movies to import data by using cmd*/
create table movies
(ID int,
Title char(200),
Year_of_release int,
Target_Age char(5),
IMDb_rating float,
Rotten_tomatoes_rating int,
Netflix int,
Prime_Video int,
Disney int,
Runtime_in_mins float,
Genre_Crime int,
Genre_Drama int,
Genre_Romance int,
Genre_Action int,
Genre_Comedy int,
Genre_Adventure int,
Genre_Sci_Fi int,
Genre_Thriller int,
Genre_Animation int,
Language_Hindi int,
Language_English int,
Language_Other int,
constraint movies_pk primary key(ID)
);

/*Price and size info of each OTT in each country*/
select Country, Netflix_avg_cost_per_month, prime_video_avg_cost_per_month, Disney_avg_cost_per_month,
       Netflix_total_lib_size, prime_video_total_lib_size, Disney_total_lib_size,
       round((Netflix_total_lib_size/Netflix_avg_cost_per_month),2) as Netflix_library_size_per_dollar,
       round((prime_video_total_lib_size/prime_video_avg_cost_per_month),2) as prime_video_library_size_per_dollar,
       round((Disney_total_lib_size/Disney_avg_cost_per_month),2) as Disney_library_size_per_dollar
from otts;

/*Price and size info of each OTT in India*/
select Netflix_avg_cost_per_month, prime_video_avg_cost_per_month, Disney_avg_cost_per_month,
       Netflix_total_lib_size, prime_video_total_lib_size, Disney_total_lib_size, 
	   round((Netflix_total_lib_size/Netflix_avg_cost_per_month),2) as Netflix_library_size_per_dollar,
	   round((prime_video_total_lib_size/prime_video_avg_cost_per_month),2) as prime_video_library_size_per_dollar,
	   round((Disney_total_lib_size/Disney_avg_cost_per_month),2) as Disney_library_size_per_dollar
from otts
where country = 'India';

/*Target Audience of Different OTTs*/
select table_12.Target_Age, Total_movies_Netflix, Total_movies_Prime_Video, Total_movies_Disney
from (select table_1.Target_Age, Total_movies_Netflix, Total_movies_Prime_Video
      from (select Target_Age, count(Target_Age) as Total_movies_Netflix
            from movies
            where Netflix = 1
            group by Target_Age
	       ) as table_1
      inner join
      (select Target_Age, count(Target_Age) as Total_movies_Prime_Video
      from movies
      where Prime_Video = 1
      group by Target_Age
      ) as table_2
      on table_1.Target_Age = table_2.Target_Age
      ) as table_12
inner join
(select Target_Age, count(Target_Age) as Total_movies_Disney
from movies
where Disney = 1
group by Target_Age
) as table_3
on table_12.Target_Age = table_3.Target_Age;

/*Average rating of movies avaialbe on different OTTs*/
/*1.Average rating of movies on netflix*/
select 'Netflix' as OTT,
round(avg(IMDb_rating),2) as avg_IMDb_rating, round(avg(Rotten_tomatoes_rating),2) as avg_Rotten_tomatoes_rating
from movies
where Netflix = 1;
/*2.Average rating of movies on Prime_Video*/
select 'Prime_Video' as OTT,
round(avg(IMDb_rating),2) as avg_IMDb_rating, round(avg(Rotten_tomatoes_rating),2) as avg_Rotten_tomatoes_rating
from movies
where Prime_Video = 1;
/*3.Average rating of movies on Disney*/
select 'Disney' as OTT,
round(avg(IMDb_rating),2) as avg_IMDb_rating, round(avg(Rotten_tomatoes_rating),2) as avg_Rotten_tomatoes_rating
from movies
where Disney = 1;

/*No of movies having recent year of release on different OTTs*/
select t_12.Year_of_release, Total_movies_Netflix, Total_movies_Prime_Video, Total_movies_Disney
from (select t_1.Year_of_release, Total_movies_Netflix, Total_movies_Prime_Video
      from (select Year_of_release, count(Year_of_release) as Total_movies_Netflix
            from movies
            where Netflix = 1
            group by Year_of_release
	       ) as t_1
      inner join
      (select Year_of_release, count(Year_of_release) as Total_movies_Prime_Video
      from movies
      where Prime_Video = 1
      group by Year_of_release
      ) as t_2
      on t_1.Year_of_release = t_2.Year_of_release
      ) as t_12
inner join
(select Year_of_release, count(Year_of_release) as Total_movies_Disney
from movies
where Disney = 1
group by Year_of_release
) as t_3
on t_12.Year_of_release= t_3.Year_of_release
order by Year_of_release;

/*Language rich OTT platforms*/
/*1.In Netflix*/
select temp_1.OTT, Total_Hindi_movies, Total_English_movies
from (select 'Netflix' as OTT, count(Language_Hindi) as Total_Hindi_movies
      from movies 
      where Netflix = 1 and Language_Hindi = 1
      ) as temp_1
inner join      
(select 'Netflix' as OTT, count(Language_English) as Total_English_movies
from movies 
where Netflix = 1 and Language_English = 1
) as temp_2
on temp_1.OTT = temp_2.OTT;
/*2.In Prime_Video*/
select temp_1.OTT, Total_Hindi_movies, Total_English_movies
from (select 'Prime_Video' as OTT, count(Language_Hindi) as Total_Hindi_movies
      from movies 
      where Prime_Video = 1 and Language_Hindi = 1
      ) as temp_1
inner join      
(select 'Prime_Video' as OTT, count(Language_English) as Total_English_movies
from movies 
where Prime_Video = 1 and Language_English = 1
) as temp_2
on temp_1.OTT = temp_2.OTT;
/*3.In Disney*/
select temp_1.OTT, Total_Hindi_movies, Total_English_movies
from (select 'Disney' as OTT, count(Language_Hindi) as Total_Hindi_movies
      from movies 
      where Disney = 1 and Language_Hindi = 1
      ) as temp_1
inner join      
(select 'Disney' as OTT, count(Language_English) as Total_English_movies
from movies 
where Disney = 1 and Language_English = 1
) as temp_2
on temp_1.OTT = temp_2.OTT;

/*Genre rich OTT platforms*/
/*1.In Netflix*/
select 'Netflix' as OTT, (select count(Genre_Crime)
from movies 
where Netflix = 1 and Genre_Crime = 1) as Total_Crime_movies, 
(select count(Genre_Drama)
from movies 
where Netflix = 1 and Genre_Drama = 1)  as Total_Drama_movies,
(select count(Genre_Romance)
from movies 
where Netflix = 1 and Genre_Romance = 1)  as Total_Romance_movies,
(select count(Genre_Action)
from movies 
where Netflix = 1 and Genre_Action = 1)  as Total_Action_movies,
(select count(Genre_Comedy)
from movies 
where Netflix = 1 and Genre_Comedy = 1)  as Total_Comedy_movies,
(select count(Genre_Adventure)
from movies 
where Netflix = 1 and Genre_Adventure = 1)  as Total_Adventure_movies,
(select count(Genre_Sci_Fi)
from movies 
where Netflix = 1 and Genre_Sci_Fi = 1)  as Total_Sci_Fi_movies,
(select count(Genre_Thriller)
from movies 
where Netflix = 1 and Genre_Thriller = 1)  as Total_Thriller_movies,
(select count(Genre_Animation)
from movies 
where Netflix = 1 and Genre_Animation = 1)  as Total_Animation_movies;
/*2.In Prime_Video*/
select 'Prime_Video' as OTT, (select count(Genre_Crime)
from movies 
where Prime_Video = 1 and Genre_Crime = 1) as Total_Crime_movies, 
(select count(Genre_Drama)
from movies 
where Prime_Video = 1 and Genre_Drama = 1)  as Total_Drama_movies,
(select count(Genre_Romance)
from movies 
where Prime_Video = 1 and Genre_Romance = 1)  as Total_Romance_movies,
(select count(Genre_Action)
from movies 
where Prime_Video = 1 and Genre_Action = 1)  as Total_Action_movies,
(select count(Genre_Comedy)
from movies 
where Prime_Video = 1 and Genre_Comedy = 1)  as Total_Comedy_movies,
(select count(Genre_Adventure)
from movies 
where Prime_Video = 1 and Genre_Adventure = 1)  as Total_Adventure_movies,
(select count(Genre_Sci_Fi)
from movies 
where Prime_Video = 1 and Genre_Sci_Fi = 1)  as Total_Sci_Fi_movies,
(select count(Genre_Thriller)
from movies 
where Prime_Video = 1 and Genre_Thriller = 1)  as Total_Thriller_movies,
(select count(Genre_Animation)
from movies 
where Prime_Video = 1 and Genre_Animation = 1)  as Total_Animation_movies;
/*3.In Disney*/
select 'Disney' as OTT, (select count(Genre_Crime)
from movies 
where Disney = 1 and Genre_Crime = 1) as Total_Crime_movies, 
(select count(Genre_Drama)
from movies 
where Disney = 1 and Genre_Drama = 1)  as Total_Drama_movies,
(select count(Genre_Romance)
from movies 
where Disney = 1 and Genre_Romance = 1)  as Total_Romance_movies,
(select count(Genre_Action)
from movies 
where Disney = 1 and Genre_Action = 1)  as Total_Action_movies,
(select count(Genre_Comedy)
from movies 
where Disney = 1 and Genre_Comedy = 1)  as Total_Comedy_movies,
(select count(Genre_Adventure)
from movies 
where Disney = 1 and Genre_Adventure = 1)  as Total_Adventure_movies,
(select count(Genre_Sci_Fi)
from movies 
where Disney = 1 and Genre_Sci_Fi = 1)  as Total_Sci_Fi_movies,
(select count(Genre_Thriller)
from movies 
where Disney = 1 and Genre_Thriller = 1)  as Total_Thriller_movies,
(select count(Genre_Animation)
from movies 
where Disney = 1 and Genre_Animation = 1)  as Total_Animation_movies;



