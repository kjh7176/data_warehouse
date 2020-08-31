# Data Modeling with Postgres
This is the third project of Udacity's **Data Engineering Nanodegree**:mortar_board:.  
The purpose is to build an **ETL pipeline** transferring data from `JSON` to `Postgres database` Using `Python` and `SQL`

## Background
A startup called :musical_note:*Sparkify* wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.  
The analytics team is particularly interested in understanding what songs users are listening to.  
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

## File Description
- `create_tables.py` drops and creates your tables. You run this file to reset your tabels before each time you run your ETL scripts.
- `etl.py` reads and processes files from **data/song_data** and **data/log_data** and loads them into your tables. You can fill this out based on your work in the ETL notebook.
- `test.ipynb` displays the first few rows of each table to let you check your database.
- `etl.ipynb` reads and processes a single file from **data/song_data** and **data/log_data** and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `sql_queries.py`contains all your sql queries, and is imported into the last three files above.


## ETL Pipeline
- Database Schema Diagram  
![ERD](/images/db_schema.PNG "ERD from https://dbdiagram.io/")
- Click [here](https://github.com/kjh7176/data_modeling_with_postgres/wiki/ETL-Pipeline) for the details of **data transformation** process used in this project.

## Usage
 1. Copy
```
$ git clone https://github.com/kjh7176/data_modeling_with_postgres

# change current working directory
$ cd data_modeling_with_postgres
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
   Open `test.ipynb` and run all code cells in order to check if records were sucessfully inserted in each table.

## Example
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
