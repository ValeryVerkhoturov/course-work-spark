from pyspark.context import SparkContext

politicians = ["Путин", "Медведев", "Песков", "Лавров", "Шойгу", "Мантуров", "Новак", "Набиуллин",
               "Голиков", "Козак", "Мединский", "Колокольцев", "Васильев", "Патрушев", "Жириновск",
               "Силуанов", "Собянин", "Рогозин"]


class SparkRDD:
    def __init__(self):
        self.tweets = None
        self.users_tweets = None
        self.sorted_tweets = None

    def set_tweets(self, path):
        sc = SparkContext().getOrCreate()
        self.tweets = sc.textFile(path).map(lambda line: line[1:-1].split('","'))
        self.tweets = self.tweets.map(lambda element: (element[1], element[10], element[12]))

    @staticmethod
    def filter_func(row):
        if row[1] == 'ru':
            for politician in politicians:
                if politician in row[2]:
                    return True
        return False

    # Фильтрация данных.
    def filter_tweets(self):
        self.tweets = self.tweets.filter(SparkRDD.filter_func)
        self.tweets = self.tweets.map(lambda element: (element[0], str(element[2])))

    # Получение userid, который чаще остальных упоминающего фамилии
    # российских политических деятелей (на русском).
    def get_userid(self):
        self.users_tweets = self.tweets.groupByKey().mapValues(len)
        self.sorted_tweets = self.users_tweets.sortBy(lambda element: element[1], ascending=False)
        return self.sorted_tweets.max(lambda element: element[1])[0]


if __name__ == '__main__':
    # Найти пользователя из РФ, чаще остальных упоминающего фамилии
    # российских политических деятелей (на русском). (2 задание)

    # 2570574680

    # file_path = 'ira_tweets_csv_hashed-20.csv'
    file_path = 'hdfs:///user/valery_v/pr5/input/ira_tweets_csv_hashed.csv'
    sparkRDD = SparkRDD()
    sparkRDD.set_tweets(file_path)
    sparkRDD.filter_tweets()
    print("USER_ID: " + sparkRDD.get_userid())
