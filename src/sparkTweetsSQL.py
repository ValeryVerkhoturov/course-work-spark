from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

politicians = ["Путин", "Медведев", "Песков", "Лавров", "Шойгу", "Мантуров", "Новак", "Набиуллин",
               "Голиков", "Козак", "Мединский", "Колокольцев", "Васильев", "Патрушев", "Жириновск",
               "Силуанов", "Собянин", "Рогозин"]

class SparkSQL:
    def __init__(self):
        self.tweets = None
        self.tweets_table = None
        sc = SparkContext.getOrCreate()
        self.spark = SparkSession(sc)

    def set_tweets(self, path):
        self.tweets = self.spark.read.csv(path, header=True)
        self.tweets.createOrReplaceTempView("tweets")

    def tweets_temp(self):
        query = """\
        CREATE OR REPLACE TEMP VIEW tweets_temp AS
          SELECT tweetid, userid, tweet_text, account_language
            FROM tweets WHERE account_language == 'ru' and tweet_language == 'ru'
        """
        self.spark.sql(query)

    def get_userid(self):
        query = f"SELECT userid FROM tweets_temp WHERE tweet_text LIKE '%{politicians[0]}%'"
        for politician in politicians[1:]:
            query += f" OR tweet_text LIKE '%{politician}%'"
        query += f" GROUP BY userid ORDER BY COUNT(tweetid) DESC LIMIT 1"
        return self.spark.sql(query).show()


if __name__ == '__main__':
    # Найти пользователя из РФ, чаще остальных упоминающего фамилии
    # российских политических деятелей (на русском). (2 задание)

    # 2570574680

    # file_path = "ira_tweets_csv_hashed.csv'
    file_path = 'hdfs:///user/valery_v/pr5/input/ira_tweets_csv_hashed.csv'
    sparkSQL = SparkSQL()
    sparkSQL.set_tweets(file_path)
    sparkSQL.tweets_temp()
    print(sparkSQL.get_userid())
