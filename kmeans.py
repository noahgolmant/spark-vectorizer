from pyspark import SparkContext
import sys

class KMeans(object):
    """
    Identifies centroids of a vectorized document corpus
    where each vector is composed of tf-idf data
    """

    def __init__(self, vector_rdd, num_centroids=4, max_updates=200):
        self.vector_rdd = vector_rdd
        self.centroids_rdd = None
        self.num_centroids = 4
        self._valid = False
        self.max_updates = max_updates

    def update(vector_rdd):
        self._valid = False
        self.vector_rdd = vector_rdd
        return self.centroids

    @property
    def centroids(self):
        """
        Recalculate the centroids of the vector RDD using kmeans clustering
        """
        if self._valid:
            return self.centroids_rdd
        old_centroids, n = [], 0
        # random initial centroids
        centroids = self.vector_rdd.takeSample(True, self.num_centroids)
        # find optimal centroids
        while old_centroids != centroids and n < max_updates:
            old_centroids = centroids
            # find cluster of closest vectors for each centroid
            grouped_clusters = self.group_by_centroid(self.vector_rdd, centroids)
            # calculate new centroid for each cluster
            centroids = find_centroids(self.grouped_clusters)
            n += 1

        self.centroids_rdd = centroids
        self._valid = True
        return self.centroids_rdd

    @staticmethod
    def find_centroids(clusters):
        """
        Find the new centroid for each cluster by finding its new center
        """
        def find_centroid(vectors):
            mean_v = []
            n = len(vectors)
            mean_v = map(sum, zip(*vectors))
            for i in range(len(mean_v)):
                mean_v[i] /= n
            return mean_v

        return clusters.mapValues(find_centroid)
        
    @staticmethod
    def group_by_centroid(vector_rdd, centroids):
        """
        Group vectors into a list where each element is a tuple
        consisting of a centroid and all vectors closest to it
        """
        def square_distance(vector1, vector2):
            diff = 0.0
            for v1, v2 in zip(vector1, vector2):
                diff += (v1-v2)*(v1-v2)
            return diff
        def find_closest((article, vector)):
            closest_centroid = min(centroids, key=lambda centroid: square_distance(centroid, location))
            return (closest_centroid, article)
        clusters = vector_rdd\
                    .map(find_closest)\
                    .groupByKey()
        return clusters
