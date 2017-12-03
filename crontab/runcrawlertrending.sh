#!/bin/bash
#!/usr/bin/env python3
#cd $HOME/Desktop/imdb_trending_new
cd $HOME/Desktop/engine_recommendation.git/trunk/crawler-module
rm -r save
mkdir save
cd save
# spark-submit $HOME/Desktop/DEMO-PROJECT.git/branches/dev/engine_recommendation/updateTrending.py $HOME/Desktop/project-crawler.git/trunk/imdb_trending_new/save/trending.csv
scrapy crawl trending -o trending.csv && spark-submit $HOME/Desktop/engine_recommendation.git/trunk/process-cassandra/updateTrending.py $HOME/Desktop/engine_recommendation.git/trunk/crawler-module/save/trending.csv
#dt=$(date '+%Y%M%d%H%m%S');
#scrapy crawl trending -o $dt.csv
#dt=$(date '+%Y%M%d%H%m%S');
#echo "$dt"
