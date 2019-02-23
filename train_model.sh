DATA_HOME=/data/11090572/word2vec
HANLP_DATA_ARCHIVE=$DATA_HOME/data-for-1.7.2.zip
DEPENDENT_JARS=lib/hanlp-portable-1.2.8.jar;lib/nak_2.11-1.3.jar;lib/breeze-natives_2.11-0.8.jar;lib/breeze-macros_2.11-0.11.2.jar;lib/breeze-config_2.11-0.9.1.jar;lib/breeze_2.11-0.11.2.jar
WORKING_DIR_FILES=stopWords
MAIN_CLASS=MLlib.Word2VecCNPlay

HADOOP_STOPWORDS_FILE=word2vec/stopWords
HADOOP_CORPUS_FILE=word2vec/news.txt

HADOOP_SEGMENTED_OUT_DIR=word2vec/segmented_doc
HADOOP_MODEL_DIR=word2vec/model



echo spark-submit --archives $HANLP_DATA_ARCHIVE -jars $DEPENDENT_JARS --class $MAIN_CLASS --name BJ_AI_WORD2VEC --files $WORKING_DIR_FILES scratch1.jar $HADOOP_STOPWORDS_FILE $HADOOP_CORPUS_FILE $HADOOP_SEGMENTED_OUT_DIR $HADOOP_MODEL_DIR
