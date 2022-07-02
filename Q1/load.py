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
#print(movie[['movieId', 'title']])

#Q1['movieId'] = movie['']
#print(tag)