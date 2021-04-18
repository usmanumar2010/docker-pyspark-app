# docker-pyspark-app

**Terminal 1** : run this commad


  - sudo docker build --no-cache -t  spark/app .


  - sudo docker-compose up

**Terminal 2** : run this command 

  - sudo docker exec docker-spark-master_master_1 /bin/bash bin/spark-submit /home/covid_app/code/main.py
