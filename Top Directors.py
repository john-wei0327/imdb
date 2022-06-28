import sys
import psycopg2

# set up some globals

usage = "Usage: q1.py [N]"
db = None

# process command-line args

argc = len(sys.argv)

# error checking
if argc == 2 and sys.argv[1].isnumeric():
	n = int(sys.argv[1])
	if n < 1:
		print(usage)
		exit(1)
elif argc == 1:
	n = 10
else:
	print(usage)
	exit(1)

# queries

most_movies = """
	select count(m.title) as movie_count, n.name
	from Movies m
	join Crew_roles cr on (cr.movie_id = m.id)
	join Names n on (n.id = cr.name_id)
	where cr.role = 'director'
	group by n.name
	order by movie_count desc, n.name 	 
"""

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()
	
	cur.execute(most_movies)
	all_movies = cur.fetchmany(n)
	
	# printing name of top N people with most movie directed
	for movie in all_movies:
		movie_count, name = movie
		print(f"{movie_count} {name}")

	cur.close()

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()