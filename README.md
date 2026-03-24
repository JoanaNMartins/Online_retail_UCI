# Online_retail_UCI
Small project to create ETL processes using PostgreSQL, Python and dashboards using Tableau.

## Project Objective

Turning simple invoice information into key business information, organizing this information into an organized relational database, following basic design principles. This project is thought out to include the possibility to expand the information present and allow for scalability in the future. Primary keys were attributed according to the codes already established, in order to allow easier transferability between recovered excel records and final analysis.

**Mission statement:** Online retail database is used to maintain generated data and provide information to support business decisions in order to improve the service provided to the clients and overall management of the business on client, product and sales information. 

**Mission objectives:**
* 

## Raw Data used
The link to the data used can be found [here](https://archive.ics.uci.edu/dataset/352/online+retail) along with additional information.

## Database Diagram
The database diagram planned for the project can be found [here](https://www.drawdb.app/editor?shareId=71857eb61c1bc3b1560033eddf2edc17).

## File Guide

### data_processing.py
Python script to extract the data from the excel file and organize it into the tables that will be used to build the database. The data here was checked for nulls, data types were casted and other errors were caught. Database independent.

**Future improvements:** either here or using the full database, one could calculate the first and last date purchased for any client, as well as the total lifetime value of that client and generate a loyalty points system. 

### database.py
Uses postgreSQL and psycopg2 to manipulate it. Assumes that desired DB was created manually and creates the tables necessary and all the schema and other parameters for further application



### run_etl.py
File that actually runs everything and loads it into the database.