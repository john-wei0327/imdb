import sys
import psycopg2

# define any local helper functions here

def print_extra(reg, lang, extra, title):
	
	print(f"'{title}'", end = " ")

	# if region and language exists 
	if reg != None and lang != None:
		print(f"(region: {reg.strip()}, language: {lang.strip()})")
	# if region exists and language doesn't exist
	elif reg != None and lang == None:
		print(f"(region: {reg.strip()})")
	# if language exists and region doesn't exist
	elif reg == None and lang != None:
		print(f"(language: {lang.strip()})")
	# if both region and language doesn't exist
	elif reg == None and lang == None:
		print(f"({extra.strip()})")
	else:
		print("")

# set up some globals

usage = "Usage: q2.py 'PartialMovieTitle'"
db = None

# process command-line args

argc = len(sys.argv)
if argc != 2:
	print(usage)
	exit(1)

pattern = sys.argv[1]

# queries

get_movies = """
	select rating, start_year, title
	from Movies
	where title ~* %s
	order by rating desc, start_year asc, title
"""

get_aliases = """
	select a.region, a.local_title, a.language, a.extra_info
	from Movies m
	join Aliases a on (a.movie_id = m.id)
	where m.title ~* %s 
	order by a.ordering
"""

# manipulate database

try:
	db = psycopg2.connect("dbname=imdb")
	cur = db.cursor()
	
	cur.execute(get_movies, [pattern])
	all_movies = cur.fetchall()

	# if there are no movies matching
	if len(all_movies) == 0:
		print(f"No movie matching '{pattern}'")
		exit(1)

	# multiple movies matching
	elif len(all_movies) > 1:
		print(f"Movies matching '{pattern}'")
		print("===============")
		for movie in all_movies:
			rating, start_year, title = movie
			print(f"{rating} {title} ({start_year})")

	# one movie matching
	else:
		rating, start_year, title = all_movies[0]
		print(f"{title} ({start_year})", end = " ")

		cur.execute(get_aliases, [pattern])
		all_aliases = cur.fetchall()

		# movie has no aliases
		if len(all_aliases) == 0:
			print("has no alternative releases")
			
		# printing aliases
		else:
			print("was also released as")
			for alias in all_aliases:
				region, local_title, language, extra_info = alias
				print_extra (region, language, extra_info, local_title)

	cur.close()	

except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()