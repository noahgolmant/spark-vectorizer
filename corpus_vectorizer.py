from pyspark import SparkContext
import sys
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from newspaper import Article
import string
from math import log10

class CorpusVectorizer(object):
    """
    Processes a corpus of news articles and produces 
    a sparse collection of document vectors containing
    tf-idf (term frequency - inverse document frequency) information.
    """

    def __init__(self, input_rdd):
        self.input_rdd = input_rdd
        self.vector_rdd = None

    def vectorize_corpus(self):
        articles = self.initialize_articles(self.input_rdd)
        terms    = self.get_terms(articles)
        idfs     = self.get_idfs(terms, articles.count())
        self.vector_rdd = self.get_article_vectors(terms, idfs)
        return self.vector_rdd

    @staticmethod
    def initialize_articles(input_rdd):
        """ Initializes articles to (title, text[]) key-value pair """
        def emit_article(line):
            """ Emit (title, link) pair """
            try:
                """ Expected format of each line in input file is: article_id URL"""
                all = line.split(',')
                title = all[0]
                text  = all[1:]
                return (title, text)
            except:
                print >> sys.stderr, "Invalid line formatting in input RDD"
                return tuple()
            
        articles = input_rdd.map(emit_article)
        return articles

    @staticmethod
    def get_terms(articles_rdd):
        def calc_tf(term, text):
            f = text.count(term)
            return 1. + log10(f) if f > 0 else 0
        
        def get_tfs((title, text)):
            return [(term, (title, calc_tf(term, text))) for term in text]
        
        return articles_rdd.flatMap(get_tfs)

    @staticmethod
    def get_idfs(terms, num_documents):
        def scale((term, rest)):
            return (term, log10(1+(num_documents / (1+dfs[term]))))
        dfs = terms.countByKey()
        return terms.map(scale)

    @staticmethod
    def get_article_vectors(terms, idfs):
        def reorder((term, ((id, tf), idf))):
            return (id, (term, tf*idf))
        return terms.join(idfs)\
                .map(reorder)\
                .groupByKey()

