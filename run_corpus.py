import sys
from corpus_vectorizer import CorpusVectorizer
from kmeans import KMeans

def process_articles(input_file, num_partitions=8):
    sc = SparkContext()
    try:
        input_rdd = sc.textFile(input_file)
        vectorized_docs = CorpusVectorizer(input_rdd).vectorize_corpus()
        centroids       = KMeans(vectorized_docs).centroids
        print >> sys.stdout, centroids.take(4)
    except Exception as e:
        print >> sys.stderr, "Unable to load file"
        print >> sys.stderr, e
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        process_articles(input_file)
    else:
        print >> sys.stderr, "No file specified"
        sys.exit(0)
