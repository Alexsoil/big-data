import pandas as pd
import os


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
    genome.to_pickle("pickleJar" + os.path.sep + "genome_tags.pkl")
    movie.to_pickle("pickleJar" + os.path.sep + "movie.pkl")
    rating.to_pickle("pickleJar" + os.path.sep + "rating.pkl")
    tag.to_pickle("pickleJar" + os.path.sep + "tag.pkl")
    print("Data loaded and saved to pickleJar")

# movies by recent score
# movies by keyword sorted by score
# movies by genre sorted by score/release date
# movies information by name (genre, score, top5 tags)
# movies by tag sorted by score (top-n)

print(genome)
print(movie)
print(rating)
print(tag)