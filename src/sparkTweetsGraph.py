import sys
from graphframes import GraphFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


class SparkGraphFrames:
    def __init__(self):
        self.sc = SparkContext.getOrCreate()
        self.sc.setCheckpointDir("hdfs:///tmp")
        self.spark = SparkSession(self.sc)
        self.gf = None
        self.tweets = None
        self.filtered_users = None

    def set_graphframe(self, path):
        self.tweets = self.spark.read.csv(path, header=True)
        self.tweets.createGlobalTempView("users")
        self.filtered_users = self.spark.sql(
            "SELECT * FROM global_temp.users WHERE user_reported_location LIKE '%Россия%'")
        vertices = self.filtered_users.select('userid').toDF('id')
        edges = self.filtered_users.select('userid', 'tweetid').toDF('src', 'dst') // tweet to reply
        edges = edges.filter(edges.dst != 'null')
        self.gf = GraphFrame(vertices, edges)

    def get_components(self):
        components = self.gf.connectedComponents()
        components.createOrReplaceTempView("components")

    def get_max_component(self):
        self.get_components()
        query = """\
        SELECT component, COUNT(*) AS count FROM components
            GROUP BY component ORDER BY count DESC
        """
        return self.spark.sql(query).show()


if __name__ == '__main__':
    # Найти наибольшую компоненту связности социального графа (группу пользователей,
    # которые общаются преимущественно друг с другом) для российских пользователей. (2 задание)

    # file_path = 'ira_tweets_csv_hashed-20.csv'
    file_path = 'hdfs:///user/valery_v/pr5/input/ira_tweets_csv_hashed.csv'
    sparkGraphFrame = SparkGraphFrames()
    sparkGraphFrame.set_graphframe(file_path)
    print(sparkGraphFrame.get_max_component())
