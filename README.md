# data_2_bots_de_assessment

The [`cleanup.ipynb`](https://github.com/Akawi85/data2bots/blob/main/cleanup.ipynb) file implements the complete solution in a single notebook.  

Simply run this all cells in this notebook to replicate

### Replicate steps for orchestration
In order to implement batch processing, I implemented these steps in separate parts and containerised using docker
and orchestrated the workflow using Airflow.

The orchestration was implemented by running Airflow in docker instead of running airflow locally

1. Install Docker on your machine by following the [link](https://docs.docker.com/engine/install/) (First check your system compatibility and requirements)  
2. Install Docker compose by following the [link](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04)  
3. Clone this repo
4. `cd` into the repo and run the command `docker compose up`
5. This spins up Airflow in docker, installs all the necessary dependencies and generates a link to the Airflow UI
6. Optionally, you can remove the environment variable that was set explicitly and rather set them in the UI by following instructions on this [link](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) . This ensures that the credentials are safe if this were to be a public repository  
7. Unpause the DAG and the entire ETL steps are executed in sequential steps with a schedule of 9AM and 5PM everyday

### Areas of improvements

Some additional improvements can be added. For instance, I am currently working on a notebook that implement the solution using pyspark which is a distributed computing framework. You can find my progress so far on this in the [`cleanup_spark`]() directory which contains a [`cleanup_spark`]() notebook of the solution. This is helpful when the amount of data grows up so large that it may require running the ETL jobs on a cluster of computers in order to achieve optimal efficiency.

This can also be orchestrated to have the spark jobs run on Google dataproc or AWS EMR for optimal scalability
