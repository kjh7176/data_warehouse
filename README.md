# Data Warehouse
This is the third project of Udacity's **Data Engineering Nanodegree**:mortar_board:.  
The purpose is to build an **ETL pipeline** that extracts data from `S3`, stages them in `Redshift`, and transforms data into a set of dimensional tables.

## Background
A startup called :musical_note:*Sparkify* wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.  
The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

## File Description
- `create_tables.py` creates a cluster if not exists, drops and creates fac and dimension tables for the star schema in Redshift. You run this file to reset your tabels before each time you run your ETL scripts.
- `etl.py` loads data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- `data_quality_check.ipynb` checks data insertions and uniqueness of primary key in each table.
- `analysis.ipynb` executes some analytic queries on tables and measure the improvement of distribution style.
- `iac.py` is a module for creating or deleting a Redshift cluster.
- `sql_queries.py`contains all sql queries, and is imported into the files above.


## Database Schema
![ERD](/images/db_schema.PNG "ERD from https://dbdiagram.io/")
<table>
    <tr>
        <th>tablename</th>
        <th>tbl_rows</th>
        <th>sortkey1</th>
        <th>diststyle</th>
    </tr>
    <tr>
        <td>artists</td>
        <td>9553</td>
        <td>artist_id</td>
        <td>ALL</td>
    </tr>
    <tr>
        <td>songplays</td>
        <td>309</td>
        <td>start_time</td>
        <td>KEY(song_id)</td>
    </tr>
    <tr>
        <td>songs</td>
        <td>14896</td>
        <td>None</td>
        <td>KEY(song_id)</td>
    </tr>
    <tr>
        <td>time</td>
        <td>6813</td>
        <td>start_time</td>
        <td>ALL</td>
    </tr>
    <tr>
        <td>users</td>
        <td>96</td>
        <td>user_id</td>
        <td>ALL</td>
    </tr>
</table>

## Usage
 1. Copy
```
$ git clone https://github.com/kjh7176/data_warehouse

# change current working directory
$ cd data_warehouse
```

 2. Create Database and Tables
```
$ python create_tables.py
```

 3. Execute ETL process
```
$ python etl.py
```

 4. Confirm  
   Open `data_quality_check.ipynb` and `analysis.ipynb` in order to test.

## Example Analytics
#### 1. Display a play list of the specific user in the latest played order.
<table>
    <tr>
        <th>title</th>
        <th>artist</th>
        <th>play_date</th>
    </tr>
    <tr>
        <td>Rianna</td>
        <td>Fisher</td>
        <td>2018-11-28</td>
    </tr>
    <tr>
        <td>I CAN&#x27;T GET STARTED</td>
        <td>Ron Carter</td>
        <td>2018-11-27</td>
    </tr>
    <tr>
        <td>Shimmy Shimmy Quarter Turn (Take It Back To Square One)</td>
        <td>Hellogoodbye</td>
        <td>2018-11-26</td>
    </tr>
    <tr>
        <td>Emergency (Album Version)</td>
        <td>Paramore</td>
        <td>2018-11-26</td>
    </tr>
    <tr>
        <td>What It Ain&#x27;t</td>
        <td>Josh Turner</td>
        <td>2018-11-26</td>
    </tr>
    <tr>
        <td>Eye Of The Beholder</td>
        <td>Metallica</td>
        <td>2018-11-26</td>
    </tr>
    <tr>
        <td>Loneliness</td>
        <td>Tomcraft</td>
        <td>2018-11-24</td>
    </tr>
    <tr>
        <td>Bang! Bang!</td>
        <td>The Knux</td>
        <td>2018-11-24</td>
    </tr>
    <tr>
        <td>You&#x27;re The One</td>
        <td>Dwight Yoakam</td>
        <td>2018-11-24</td>
    </tr>
    <tr>
        <td>Sun / C79</td>
        <td>Cat Stevens</td>
        <td>2018-11-24</td>
    </tr>
    <tr>
        <td>Wax on Tha Belt (Baby G Gets Biz)</td>
        <td>Mad Flava</td>
        <td>2018-11-24</td>
    </tr>
    <tr>
        <td>Catch You Baby (Steve Pitron &amp; Max Sanna Radio Edit)</td>
        <td>Lonnie Gordon</td>
        <td>2018-11-23</td>
    </tr>
    <tr>
        <td>Nothin&#x27; On You [feat. Bruno Mars] (Album Version)</td>
        <td>B.o.B</td>
        <td>2018-11-21</td>
    </tr>
    <tr>
        <td>Die Kunst der Fuge_ BWV 1080 (2007 Digital Remaster): Contrapunctus XVII - Inversus</td>
        <td>Lionel Rogg</td>
        <td>2018-11-21</td>
    </tr>
    <tr>
        <td>Mr. Jones</td>
        <td>Counting Crows</td>
        <td>2018-11-21</td>
    </tr>
    <tr>
        <td>You&#x27;re The One</td>
        <td>Dwight Yoakam</td>
        <td>2018-11-09</td>
    </tr>
</table>

#### 2. What is the most played song every year?
<table>
    <tr>
        <th>year</th>
        <th>title</th>
        <th>artist</th>
        <th>play_count</th>
    </tr>
    <tr>
        <td>2018</td>
        <td>You&#x27;re The One</td>
        <td>Dwight Yoakam</td>
        <td>37</td>
    </tr>
</table>

#### 3. Display the 5 most played artists from LA.
<table>
    <tr>
        <th>rank</th>
        <th>artist</th>
        <th>play_count</th>
    </tr>
    <tr>
        <td>1</td>
        <td>Linkin Park</td>
        <td>4</td>
    </tr>
    <tr>
        <td>1</td>
        <td>Metallica</td>
        <td>4</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Black Eyed Peas</td>
        <td>3</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Katy Perry</td>
        <td>1</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Maroon 5</td>
        <td>1</td>
    </tr>
</table>

#### 4. What time are women most likely to listen music?
<table>
    <tr>
        <th>hour</th>
        <th>play_count</th>
    </tr>
    <tr>
        <td>17</td>
        <td>27</td>
    </tr>
    <tr>
        <td>15</td>
        <td>18</td>
    </tr>
    <tr>
        <td>18</td>
        <td>15</td>
    </tr>
    <tr>
        <td>16</td>
        <td>14</td>
    </tr>
    <tr>
        <td>14</td>
        <td>13</td>
    </tr>
    <tr>
        <td>11</td>
        <td>13</td>
    </tr>
    <tr>
        <td>8</td>
        <td>12</td>
    </tr>
    <tr>
        <td>20</td>
        <td>12</td>
    </tr>
    <tr>
        <td>19</td>
        <td>10</td>
    </tr>
    <tr>
        <td>21</td>
        <td>10</td>
    </tr>
</table>


## Example Data
> Query  
```
SELECT * FROM songplays LIMIT 5;
```
> Result  

![songplays](/images/songplays.PNG)
   
> Query  
```
SELECT * FROM users LIMIT 5;
```
> Result  

![users](/images/users.PNG)
   
> Query  
```
SELECT * FROM songs LIMIT 5;
```
> Result  

![songs](/images/songs.PNG)

> Query  
```
SELECT * FROM artists LIMIT 5;
```
> Result  

![artists](/images/artists.PNG)
  
> Query  
```
SELECT * FROM time LIMIT 5;
```
> Result  

![time](/images/time.PNG)
