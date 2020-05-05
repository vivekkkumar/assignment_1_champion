# assignment_1_champion
Assignment

1. To run the python file.

Pyspark is a pre requisite.

Run below command
#pip instal pyspark
after installation

csv_json_converter.py input_filename, output_filename

2. SQL Query:

Assumed Table is with the below structure

id,	event_name, people_count

1, 	Football, 	20;
2, 	Football, 	30;
3, 	F1, 		40;
4, 	F1, 		30;
5, 	F1, 		30;


SELECT event_name, SUM(people_count) as event_count
FROM table
GROUP BY event_name
HAVING count(event_name) >3 and event_count > 100;
