# Mini-SQL-Engine
A mini sql engine which will run a subset of SQL queries using command line interface

## Type of Queries

1. Project Columns (could be any number of columns) from one or more tables.

`Select * from table_name;`

2. Aggregate functions : Simple aggregate functions on a single column.Sum, average, max, min and count. They will be very trivial given that the data is only integers.

`Select max(col1) from table_name;`

3. Select/project with distinct from one table (distinct of a pair of values indicates the pair should be distinct)

`Select distinct col1, col2 from table_name;`

4. Select with WHERE from one or more tables 

`Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;`

5. Select/Project Columns(could be any number of columns) from table using “group by”

`Select col1, COUNT(col2) from table_name group by col1.`

6. Select/Project Columns(could be any number of columns) from table in ascending/descending order according to a column using “order by”.

`Select col1,col2 from table_name order by col1 ASC|DESC.`
