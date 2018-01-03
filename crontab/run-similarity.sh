#!/bin/bash
# export python=python3
# export PYSPARK_PYTHON=python3
# cd $HOME/Desktop/engine_recommendation.git/trunk/output-data/similarity-item/
# rm -r result.csv
spark-submit $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/similarity_item.py $HOME/Desktop/engine_recommendation.git/trunk/meta-data/150_movies.csv $HOME/Desktop/engine_recommendation.git/trunk/output-data/similarity-item/150.csv && spark-submit $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/similarity_item.py $HOME/Desktop/engine_recommendation.git/trunk/meta-data/200_movies.csv $HOME/Desktop/engine_recommendation.git/trunk/output-data/similarity-item/200.csv && spark-submit $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/similarity_item.py $HOME/Desktop/engine_recommendation.git/trunk/meta-data/movies.csv $HOME/Desktop/engine_recommendation.git/trunk/output-data/similarity-item/movies.csv