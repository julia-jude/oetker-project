****PySpark Application Running on Airflow Using Docker****

This project is a pyspark application that uses airflow locally within Docker. 
It reads data from a URL and saves the raw data in JSON format, performs data cleansing, 
and analyses metrics on the cleansed data. 
The output metrics are then written to a output text file.

Prerequisites:

To run this project, you will need the following software installed on your local machine:

- Docker: https://www.docker.com/products/docker-desktop/,

- Docker Compose: for windows and mac, the docker-compose is already included in Docker Desktop, so you don't need to install it separately. 
You can verify the installation by running docker-compose --version.
,
- Python 3 : https://www.python.org/downloads/

Setup:
- Clone this repository to your local machine.
Open a terminal window and navigate to the project directory.

- Run the following command to build the Airflow Dockerfile and 
create the image named "extending_airflow:latest":-

docker build -t extending_airflow:latest .

- Run the following command to start the local Airflow:

docker-compose up 

- Access the Airflow web UI by opening a browser and navigating to http://localhost:8080.

- Go to the DAG named "dag_oetker_app" and trigger to execute the pyspark scripts

- Once the dag execution is completed, the output files will be found under /output folder

- To shut down the local Airflow instance, run the following command:

docker-compose down


- Other useful commands:

    (i) To list the active containers: docker ps
    
    (ii) To access the cli of a running container :  docker exec -it <container-id> bash
    
    (iii) To stop and remove all containers/images/volumes created from the current docker compose: docker-compose down --volumes --rmi all 

Author:
Julia 



