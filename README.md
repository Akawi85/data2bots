# Data2Bots Data Engineering Assessment

The [`cleanup.ipynb`](https://github.com/Akawi85/data2bots/blob/main/cleanup.ipynb) file implements the complete solution in a single notebook.  

Simply run all cells in this notebook to replicate the complete solution from a single notebook file.

### Replicate steps for orchestration
In order to implement batch processing, I implemented these steps in separate parts, containerised them using docker
and orchestrated the workflow using Airflow.

The orchestration was implemented by running Airflow in Docker instead of running airflow locally. This is to ensure that the code works the same regardless of the operating system it is running on. You only need to have docker and docker compose installed and configured correctly.

##### Steps 

1. Install Docker on your machine by following the [link](https://docs.docker.com/engine/install/) (First check your system compatibility and requirements)  
2. Install Docker compose by following the [link](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)  
3. Clone this repo
4. `cd` into the downloaded folder and run the command `docker compose up`
5. This spins up Airflow in docker, installs all the necessary dependencies and generates a link to the Airflow UI
6. Optionally, you can remove the environment variable that was set explicitly in the [batch_etl_pipeline](https://github.com/Akawi85/data2bots/blob/main/dags/airflow_dags/batch_etl_pipeline.py) DAG and rather set them in the UI by following instructions in this [link](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) . This ensures that the credentials are safe if this were to be a public repository  
7. Input the Airflow login details as:  
  **Username:** `airflow`   
  **Password:** `airflow`
8. Unpause the DAG and the entire ETL steps are executed in sequential steps with a schedule of 9AM and 5PM everyday

##### Image of Successful DAG Run

![Image of successful ETL DAG run](https://github.com/Akawi85/data2bots/blob/main/images/Screenshot%202023-06-26%20at%2019.45.53.png)

### Areas of improvements

Some additional improvements can be added. For instance, I am currently working on a notebook that implement the solution using pyspark which is a distributed computing framework. You can find my progress so far on this in the [`cleanup_spark`](https://github.com/Akawi85/data2bots/tree/main/cleanup_spark) directory which contains a [`cleanup_spark`](https://github.com/Akawi85/data2bots/blob/main/cleanup_spark/cleanup_spark.ipynb) notebook of the solution. This is helpful when the amount of data grows up so large that it may require running the ETL jobs on a cluster of computers in order to achieve optimal efficiency.

This can also be orchestrated to have the spark jobs run on Google dataproc or AWS EMR for optimal scalability
