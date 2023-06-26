# Data2Bots Data Engineering Assessment

The [`cleanup.ipynb`](https://github.com/Akawi85/data2bots/blob/main/cleanup.ipynb) file implements the complete solution in a single notebook.  

Simply run all cells in this notebook to replicate the complete solution from a single notebook file.

### Replicate steps for orchestration
In order to implement batch processing, I implemented these steps in separate parts, containerised them using docker
and orchestrated the workflow using Airflow.

The orchestration was implemented by running Airflow in Docker instead of running airflow locally.

##### Steps 

1. Install Docker on your machine by following the [link](https://docs.docker.com/engine/install/) (First check your system compatibility and requirements)  
2. Install Docker compose by following the [link](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)  
3. Clone this repo
4. `cd` into the downloaded folder and run the command `docker compose up`
5. This spins up Airflow in docker, installs all the necessary dependencies and generates a link to the Airflow UI
6. Optionally, you can remove the environment variable that was set explicitly in the [batch_etl_pipeline](https://github.com/Akawi85/data2bots/blob/main/dags/airflow_dags/batch_etl_pipeline.py) DAG and rather set them in the UI by following instructions in this [link](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) . This ensures that the credentials are safe if this were to be a public repository  
7. Unpause the DAG and the entire ETL steps are executed in sequential steps with a schedule of 9AM and 5PM everyday

![Image of successful ETL DAG run](https://github.com/Akawi85/data2bots/blob/main/images/Screenshot%2023-06-26%at%19.45.53.png)

### Areas of improvements

Some additional improvements can be added. For instance, I am currently working on a notebook that implement the solution using pyspark which is a distributed computing framework. You can find my progress so far on this in the [`cleanup_spark`](https://github.com/Akawi85/data2bots/tree/main/cleanup_spark) directory which contains a [`cleanup_spark`](https://github.com/Akawi85/data2bots/blob/main/cleanup_spark/cleanup_spark.ipynb) notebook of the solution. This is helpful when the amount of data grows up so large that it may require running the ETL jobs on a cluster of computers in order to achieve optimal efficiency.

This can also be orchestrated to have the spark jobs run on Google dataproc or AWS EMR for optimal scalability

### Note

I am soliciting for some pardon due to my late submission. This was not without a cause as I was delayed 6 days before I could get access to the database credentials. Attached is the email trail that shows when I received the assignment instructions, the mails I sent trying to request for the database credentials and the day I got a response of the database credential. 

##### When I received a mail detailing the assessment Instructions

![When I received the Assessment Instructions](https://github.com/Akawi85/data2bots/blob/main/images/Screenshot%202023-06-26%20at%2021.12.29.png) . 


##### When I requested Access to the database credentials

![When I requested database credentials](https://github.com/Akawi85/data2bots/blob/main/images/Screenshot%202023-06-26%20at%2021.13.22.png) . 


##### When I finally got the database credentials

![When I received the credentials](https://github.com/Akawi85/data2bots/blob/main/images/Screenshot%202023-06-26%20at%2021.13.55.png) . 


Although I made my initial submissions quite on time and before the deadline, I realised there were some bugs in my code that needed to be fixed in order to make the code align to the laid out instructions. And also this section of the README had to be updated. All these culminated to my late submission for which I seek a recourse and pardon.