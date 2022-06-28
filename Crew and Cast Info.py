import sys
import psycopg2

# set up some globals

usage = "Usage: q3.py 'MovieTitlePattern' [Year]"
db = None
year = None

# process command-line args

argc = len(sys.argv)

# determining if year is supplied
if argc == 2:
	pattern = sys.argv[1]
elif argc == 3:
	pattern = sys.argv[1]
	# checking for year is a number and is positive
	if sys.argv[2].isnumeric() and int(sys.argv[2]) >= 0:
		year = int(sys.argv[2])
	else: 
		print(usage)
		exit(1)
else:
	print(usage)
	exit(1)
	
# queries

get_movies = """
	select rating, start_year, title, id
	from Movies
	where title ~* %s 
	order by rating desc, start_year , title
"""

get_movies_year = """
	select rating, start_year, title, id
	from Movies
	where title ~* %s and start_year = %s
	order by rating desc, title
"""

get_actors = """
	select n.name, ar.played
	from Names n
	join Acting_roles ar on (ar.name_id = n.id)
	join Movies m on (ar.movie_id = m.id)
	join Principals p on (p.name_id = n.id and p.movie_id = m.id)
	where m.id = %s
	order by p.ordering, ar.played
"""

get_crews = """
	select n.name, cr.role
	from Names n
	join Crew_roles cr on (cr.name_id = n.id)
	join Movies m on (cr.movie_id = m.id)
	join Principals p on (p.name_id = n.id and p.movie_id = m.id)
	where m.id = %s
	order by p.ordering, cr.role
"""

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()

	# query with and without optional year
	if year == None:
		cur.execute(get_movies, [pattern])
	else:
		cur.execute(get_movies_year, [pattern, year])
	all_movies = cur.fetchall()
	
	# no movies match
	if len(all_movies) == 0:
		if year == None:
			print(f"No movie matching '{pattern}'")
			exit(1)
		else:
			print(f"No movie matching '{pattern}' {year}")
			exit(1)

	# multiple movies matching
	elif len(all_movies) > 1:
		
		# printing all matching movies
		if year == None:
			print(f"Movies matching '{pattern}'")
		else: 
			print(f"Movies matching '{pattern}' {year}")
		print("===============")
		
		for movie in all_movies:
			rating, start_year, title, movie_id = movie
			print(f"{rating} {title} ({start_year})")

	# single movie matched
	else: 
		rating, start_year, title, movie_id = all_movies[0]
		print(f"{title} ({start_year})")
		print("===============")

		# printing principal actors
		print("Starring")
		cur.execute(get_actors, [movie_id])
		all_actors = cur.fetchall()
		for actor in all_actors:
			name, played = actor
			print(f" {name} as {played}")

		# printing principal crews
		print("and with")
		cur.execute(get_crews, [movie_id])
		all_crews = cur.fetchall()
		for crew in all_crews:
			name, role = crew
			print(f" {name}: {role.capitalize().replace('_', ' ')}")

	cur.close()	
	
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()