# spark-vectorizer
Uses Apache Spark to create a basic TF-IDF model of a corpus and perform k-means clustering.

To run:
 - use "downloader.py <filename>" to download and get stemmed text of articles from a CSV where each line is an ID and an article URL.
 - submit "run_corpus.py <filename>" with spark to create a vector model and perform kmeans clustering on this new CSV.
