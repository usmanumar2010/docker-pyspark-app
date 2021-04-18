# docker-pyspark-app

This repository demonstrating a pyspark application using docker.The dataset has been downloaded from the following repository

https://github.com/owid/covid-19-data/tree/master/public/data#%EF%B8%8F-download-our-complete-covid-19-dataset--csv--xlsx--json

in the data directroy.




The transfomed data in the app/transformed_data directory look like this 

```

[{"month":"202006","AE":1274693,"BH":575360,"KW":1106772,"OM":752585,"QA":2393921,"SA":4063590,"AFRICA":7891288,"ASIA":40774922,"EUROPE":65916898,"GCC":10166921,"NORTH AMERICA":75168650,"OCEANIA":268729,"SOUTH AMERICA":46451110},{"month":"202009","AE":2450597,"BH":1856231,"KW":2885759,"OM":2740993,"QA":3670670,"SA":9799409,"AFRICA":41152839,"ASIA":244761770,"EUROPE":129182600,"GCC":23403659,"NORTH AMERICA":238147445,"OCEANIA":871260,"SOUTH AMERICA":217278558},{"month":"202101","AE":7811891,"BH":3022735,"KW":4881830,"OM":4078314,"QA":4566164,"SA":11315754,"AFRICA":99495750,"ASIA":645412827,"EUROPE":847889087,"GCC":35676688,"NORTH AMERICA":836603388,"OCEANIA":986616,"SOUTH AMERICA":450178960},{"month":"202011","AE":4535051,"BH":2539313,"KW":4082135,"OM":3601350,"QA":4075334,"SA":10591512,"AFRICA":59462318,"ASIA":426835447,"EUROPE":420986970,"GCC":29424695,"NORTH AMERICA":400666181,"OCEANIA":912716,"SOUTH AMERICA":311343963}......]

```

Install docker on linux:

   you can follow  steps from the following link to install docker:
   
      - https://docs.docker.com/engine/install/ubuntu/
      - https://docs.docker.com/compose/install/
  

After cloning this app


**Terminal 1** : run this commad


  ```- sudo docker build -t spark/app .```


  ```- sudo docker-compose up```

**Terminal 2** : in order to run pysprak script run this command 

  ```- sudo docker exec docker-spark-master_master_1 /bin/bash bin/spark-submit /home/app/code/main.py```
  
 
Other:
  
   to access container interactive shell use this command
    
   ```- sudo docker exec -it docker-spark-master_master_1 /bin/bash
    
    
