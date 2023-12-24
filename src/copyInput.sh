#!/bin/bash

hdfs dfs -rm -r pr5
hdfs dfs -mkdir pr5
hdfs dfs -mkdir pr5/input

hdfs dfs -put ira_tweets_csv_hashed.csv pr5/input/
#hdfs dfs -put tweets.csv pr5/input/
#hdfs dfs -put ira_tweets_csv_hashed-20.csv pr5/input/
