from ctypes.wintypes import tagSIZE
import pandas as pd
import os
import fnmatch

#pd.set_option('display.max_rows', None)

try:
    genome = pd.read_pickle("pickleJar" + os.path.sep + "genome_tags.pkl")
    movie = pd.read_pickle("pickleJar" + os.path.sep + "movie.pkl")
    rating = pd.read_pickle("pickleJar" + os.path.sep + "rating.pkl")
    tag = pd.read_pickle("pickleJar" + os.path.sep + "tag.pkl")
    print("Data loaded from pickleJar")
except:
    print("Pickled data not available, reading from csv...")
    genome = pd.read_csv("archive" + os.path.sep + "genome_tags.csv")
    movie = pd.read_csv("archive" + os.path.sep + "movie.csv")
    rating = pd.read_csv("archive" + os.path.sep + "rating.csv")
    tag = pd.read_csv("archive" + os.path.sep + "tag.csv")
    # Refactor movies
    title = []
    date = []
    for idx, row in movie.iterrows():
        temp = row['title'].rstrip().replace("\"", "")
        try:
            date.append(int(temp[-5:-1]))
            title.append(temp[:-6])
        except:
            date.append(3000)
            title.append(temp)
    movie['date'] = date
    movie['title'] = title
    # Refactor ratings
    timestamp = []
    print(rating)
    # Pickle results
    genome.to_pickle("pickleJar" + os.path.sep + "genome_tags.pkl")
    movie.to_pickle("pickleJar" + os.path.sep + "movie.pkl")
    rating.to_pickle("pickleJar" + os.path.sep + "rating.pkl")
    tag.to_pickle("pickleJar" + os.path.sep + "tag.pkl")
    print("Data loaded and saved to pickleJar")

# movies by recent score
# movieId title rating timestamp (movies rating)
# movies by keyword sorted by score
# movieId title rating (movies rating)
# movies by genre sorted by score/release date
# movieId title genre date rating
# movies information by name (genre, score, top5 tags)
# movies by tag sorted by score (top-n)

Q1 = pd.DataFrame

Q1 = movie[['movieId', 'title']].merge(rating[['movieId', 'rating', 'timestamp']], on='movieId', how='outer')
Q1.to_pickle("pickleJar" + os.path.sep + "Q1_table.pkl")
print(Q1)

Q2 = pd.DataFrame

Q2 = movie[['movieId', 'title']].merge(rating[['movieId', 'rating']], on='movieId', how='outer')
Q2.to_pickle("pickleJar" + os.path.sep + "Q2_table.pkl")
print(Q2)

Q3_1 = pd.DataFrame

Q3_1 = movie[['movieId', 'title', 'genres']].merge(rating[['movieId', 'rating']], on='movieId', how='outer')
Q3_1.to_pickle("pickleJar" + os.path.sep + "Q3_1_table.pkl")
print(Q3_1)

Q3_2 = pd.DataFrame

Q3_2 = movie[['movieId', 'title', 'genres','date']]
Q3_2.to_pickle("pickleJar" + os.path.sep + "Q3_2_table.pkl")
print(Q3_2)

#Q4 = pd.DataFrame

#Q4 = movie[['movieId', 'title', 'genres']].merge(rating[['movieId', 'rating']], on='movieId', how='outer')
#Q4 = Q4.merge(tag[['movieId', 'tag']], on='movieId', how='outer')
#Q4.to_pickle("pickleJar" + os.path.sep + "Q4_table.pkl")
#print(Q4)

#Q5 = pd.DataFrame

#Q5 = movie[['movieId', 'title']].merge(rating[['movieId', 'rating']], on='movieId', how='outer')
#Q5 = Q5.merge(tag[['movieId', 'tag']], on='movieId', how='outer')
#Q5.to_pickle("pickleJar" + os.path.sep + "Q5_table.pkl")
#print(Q5)

#print(movie[['movieId', 'title']])

#Q1['movieId'] = movie['']
#print(tag)