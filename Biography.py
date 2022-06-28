import sys
import psycopg2

# define any local helper functions here
def print_person(name, birth, death):
	# birth and death are valid
	if birth != None and death != None:
		print(f"{name} ({birth}-{death})") 
	# birth is valid, death is invalid
	elif birth != None and death == None:
		print(f"{name} ({birth}-)") 
	# birth is invalid
	else:
		print(f"{name} (???)") 

# set up some globals

usage = "Usage: q4.py 'NamePattern' [Year]"
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
	if sys.argv[2].isnumeric and int(sys.argv[2]) >= 0:
		year = int(sys.argv[2])
	else:
		print(usage)
		exit(1)
else:
	print(usage)
	exit(1)

# queries

get_name = """  
	select id, name, birth_year, death_year
	from Names
	where name ~* %s
	order by name, birth_year asc, id asc
"""
get_name_year = """
	select id, name, birth_year, death_year
	from Names
	where name ~* %s and birth_year = %s
	order by name, id asc
"""

get_rating = """
	select avg(m.rating)
	from Movies m
	join Principals p on (p.movie_id = m.id)
	join Names n on (p.name_id = n.id)
	where n.id = %s
"""

get_genres = """
	select mg.genre, count(m.title)
	from Movie_genres mg
	join Movies m on (m.id = mg.movie_id)
	join Principals p on (p.movie_id = m.id)
	join Names n on (n.id = p.name_id)
	where n.id = %s
	group by mg.genre
	order by count(m.title) desc, mg.genre
"""

get_movies = """
	select m.title, m.start_year, m.id
	from Movies m
	join Principals p on (p.movie_id = m.id)
	join Names n on (n.id = p.name_id)
	where n.id = %s
	order by start_year, title
"""

get_acting_roles = """
	select n.name, ar.played
	from Acting_roles ar
	join Names n on (n.id = ar.name_id)
	join Movies m on (m.id = ar.movie_id)
	where n.id = %s and m.id = %s
	order by n.name
"""

get_crew_roles = """
	select n.name, cr.role
	from Crew_roles cr
	join Names n on (n.id = cr.name_id)
	join Movies m on (m.id = cr.movie_id)
	where n.id = %s and m.id = %s
	order by n.name
"""

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()
	
	# query with and without optional year
	if year == None:
		cur.execute(get_name, [pattern])
	else:
		cur.execute(get_name_year, [pattern,year])
	names = cur.fetchall()
	
	# no match
	if len(names) == 0:
		if year == None:
			print(f"No name matching '{pattern}'")
			exit(1)
		else:
			print(f"No name matching '{pattern}' {year}")
			exit(1)

	# match more than one person
	elif len(names) > 1:
		print(f"Names matching '{pattern}'")
		print("===============")
		for person in names:
			name_id, name, birth_year, death_year = person
			print_person (name, birth_year, death_year)

	# match one person	
	else:
		name_id, name, birth_year, death_year = names[0]
		print(f"Filmography for ", end = "") 
		print_person (name, birth_year, death_year)
		print("===============")

		# personal rating
		print(f"Personal Rating:", end = " ")
		cur.execute(get_rating,[name_id]) 
		avg = cur.fetchone()
		if avg[0] == None:
			rating = 0
		else:
			rating = round((avg[0]), 1)
		print(f"{rating}")

		# top genres
		print("Top 3 Genres:")
		cur.execute(get_genres,[name_id])
		all_genres = cur.fetchmany(3)

		for category in all_genres:
			genre, count = category
			print(f" {genre}")
		
		# print filmography
		print("===============")
		cur.execute(get_movies,[name_id])
		all_movies = cur.fetchall()

		for movie in all_movies:
			title, start_year, movie_id = movie
			print(f"{title} ({start_year})") 

			# print all acting roles
			cur.execute(get_acting_roles,[name_id, movie_id])
			all_acting_roles = cur.fetchall() 
			for acting_role in all_acting_roles:
				name, role = acting_role
				print(" playing", end = " ")
				print(f"{role}")

			# print all crew roles
			cur.execute(get_crew_roles, [name_id, movie_id])
			all_crew_roles = cur.fetchall()
			for crew_role in all_crew_roles:
				name, role = crew_role 
				print(" as", end = " ")
				print(f"{role.capitalize().replace('_', ' ')}")

	cur.close()

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()