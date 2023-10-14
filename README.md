# Data Analysis with Apache Airflow ETL Pipeline

Welcome to the Airflow Data Engineer Code Challenge! This project leverages Apache Airflow to create an ETL pipeline whose final purpose is to analyze two datasets: one containing Spotify song data and the other containing Grammy nominees. The primary focus of this project is to explore the characteristics of songs nominated for Grammy Awards and answer the question, "How do Grammy-nominated songs behave?"

## Prerequisites

Before running this project, you need to have the following prerequisites in place:

- Apache Airflow
- Python
- Google Drive API credentials
- MySQL Database

## Datasets Used

### Spotify Dataset

The Spotify dataset contains information about songs, including details such as track name, artist, popularity, and other audio features. You can find this dataset in Kaggle [here](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset).

### Grammy Nominees Dataset

The Grammy nominees dataset includes data on the songs and artists nominated for Grammy Awards. You can access this dataset in Kaggle [here](https://www.kaggle.com/datasets/unanimad/grammy-awards).

## Repository Structure

The repository has the following structure:

- `requirements.txt`: This file contains the necessary dependencies for the project.
- `grammys_dashboard.pdf`: You can find the results of the analysis in this PDF document.
- `exploratory_data_analysis` folder: This directory contains the exploratory data analysis files.
- `data` folder: This folder stores the datasets used in the analysis. Here you can add the datasets mentioned and see the datset resulting from the analysis.
- `airflow` folder: The Apache Airflow workflow files are located here.

## How to Run This Project

Follow these steps to run the project:

1. Create a virtual environment:
   
   ```bash
   python -m venv venv
   ```
2. Activate the virtual environment:
   
   ```bash
   source venv/bin/activate
   ```
3. Install project dependencies:
    
   ```bash
   pip install -r requirements.txt
   ```
4. Create database credentials file 'db_config.json' in a folder called secrets with the following content:
      ```
      {
        "user": "your_username",
        "password": "your_password",
        "host": "your_host"
        "server": "your_server",
        "db": "your_database"
      }
      ```

    > **Note:** This file is necessary, by not having it you won't be able to access the database unless you state the credentials directly (not recommended). If you choose to give it a different name or location, you must change the the access route in the code.
    >
    
5. Set the AIRFLOW_HOME environment variable in the `airflow` folder:
   
   ```bash
   export AIRFLOW_HOME=$(pwd)
   ```
6. Start the Airflow scheduler
   
   ```bash
   airflow scheduler
   ```
7. Start the Airflow standalone
   
   ```bash
   airflow standalone
   ```

Now you're ready to explore and analyze music using Apache Airflow. Happy analyzing!

## Author
Maria Angelica Portocarrero Quintero
