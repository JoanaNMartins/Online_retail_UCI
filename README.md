# Online_retail_UCI
Small project to create ETL processes using PostgreSQL, Python and dashboards using Tableau.

## Project Objective

Turning simple invoice information into key business information, organizing this information into an organized relational database, following basic design principles. This project is thought out to include the possibility to expand the information present and allow for scalability in the future. Primary keys were attributed according to the codes already established, in order to allow easier transferability between recovered excel records and final analysis.

## Raw Data used
The link to the data used can be found [here](https://archive.ics.uci.edu/dataset/352/online+retail) along with additional information.

## Database Diagram
The database diagram planned for the project can be found [here](https://www.drawdb.app/editor?shareId=71857eb61c1bc3b1560033eddf2edc17).

## File Guide

### create_tables.py
Python script to extract the data from the excel file and organize it into the tables that will be used to build the database. The data here was checked for nulls, data types were casted and other errors were caught.

WIP: Needs to load the data into the database.