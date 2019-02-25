DATA_HOME=/data/11090572/word2vec
HANLP_DATA_ARCHIVE=$DATA_HOME/data-for-1.7.2.zip
DEPENDENT_JARS=lib/hanlp-portable-1.2.8.jar,lib/nak_2.11-1.3.jar,lib/breeze-natives_2.11-0.8.jar,lib/breeze-macros_2.11-0.11.2.jar,lib/breeze-config_2.11-0.9.1.jar,lib/breeze_2.11-0.11.2.jar
WORKING_DIR_FILES=stopWords,data_dependent.ini
MAIN_CLASS=MLlib.Word2VecCNPlay

spark-submit --archives $HANLP_DATA_ARCHIVE --jars $DEPENDENT_JARS --class $MAIN_CLASS --name BJ_AI_WORD2VEC --files $WORKING_DIR_FILES train_model.jar
