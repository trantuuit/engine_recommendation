#!/bin/bash
# export python=python3
# export PYSPARK_PYTHON=python3
cd $HOME/Desktop/engine_recommendation.git/trunk/output-data/similarity-item/
rm -r result.csv
spark-submit $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/similarity_item.py $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/meta-data/movie_test.csv $HOME/Desktop/engine_recommendation.git/trunk/recommendation_engine/output-data/similarity-item/result.csv