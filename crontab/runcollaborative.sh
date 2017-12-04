#!/bin/bash
cd $HOME/Desktop/engine_recommendation.git/trunk/output-data/collaborative-filtering
rm -r result.csv
spark-submit $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/collaborative_filtering.py $HOME/Desktop/engine_recommendation.git/trunk/meta-data/ratings_20.txt $HOME/Desktop/engine_recommendation.git/trunk/output-data/collaborative-filtering/result.csv && spark-submit $HOME/Desktop/engine_recommendation.git/trunk/process-cassandra/updateCollaborative.py $HOME/Desktop/engine_recommendation.git/trunk/output-data/collaborative-filtering/result.csv