## Realtime-Scalable-Data-Engineering-Pipeline

This project provides a comprehensive guide for building a scalable and real-time data engineering pipeline. It encompasses the entire data lifecycle, from data ingestion to processing and storage, utilizing a robust and industry-standard tech stack.

**System Architecture:**

*![Project Architecture](https://github.com/tejasjbansal/Realtime-Scalable-Data-Engineering-Pipeline-/assets/56173595/6f75152b-0252-470d-92e2-742932a7e160)


**Key Features:**

* **End-to-End Pipeline:** Covers the entire data lifecycle, from data ingestion to processing and storage.
* **Scalable Architecture:** Leverages containerization with Docker for easy deployment and horizontal scaling of components.
* **Robust Tech Stack:** Employs industry-standard tools like Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra.
* **Streamlined Data Flow:** Utilizes Apache Kafka for streaming data from PostgreSQL to the processing engine for real-time processing.
* **Monitoring and Management:** Includes Control Center and Schema Registry for monitoring Kafka streams and managing data schemas.

**Components:**

* **Data Source:** Random user data is generated from the `randomuser.me` API.
* **Apache Airflow:** Orchestrates the pipeline, fetches data from the API, and stores it in a PostgreSQL database.
* **Apache Kafka and Zookeeper:** Enable real-time streaming of data from PostgreSQL to the processing engine.
* **Control Center and Schema Registry:** Provide tools for monitoring Kafka streams and managing data schemas.
* **Apache Spark:** Performs data processing tasks with its master and worker nodes.
* **Cassandra:** Stores the processed data for further analysis.

**Technologies:**

* Apache Airflow
* Python
* Apache Kafka
* Apache Zookeeper
* Apache Spark
* Cassandra
* PostgreSQL
* Docker

**Getting Started:**

1. Clone the project repository.
2. Set up the required environment based on the instructions provided.
3. Configure and start the individual components of the pipeline.
4. Access the Airflow UI to monitor and manage the pipeline execution.

**Further Exploration:**

This project serves as a foundational framework for building real-time data pipelines. You can further customize and extend it based on your specific data processing needs and requirements.

**Note:**

Detailed instructions for setup, configuration, and running the pipeline are provided within the project repository.
