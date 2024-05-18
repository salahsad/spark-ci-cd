APPLICATION_CONF= "C:\Users\SALAH_SAD\IdeaProjects\spark\src\main\resources\application.conf"



spark-submit \
  --properties-file "$APP_CONF_PATH" \
  --class sparkproject.demo \
  --master local[*] \
  --deploy-mode client \
  --executor-memory 4g \
    target/spark-1.0-SNAPSHOT-jar-with-dependencies.jar