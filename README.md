# SentimentStream

An Docker based apache airflow pipeline covering following use cases:
Pipeline 1: Scrapes articles for HDFC and Tata Motors, performs sentiment analysis, and stores the results.
Pipeline 2: Analyzes the MovieLens dataset (runs only if Pipeline 1 succeeds on the same day).

The final data is stored in postgres DB.

## Installation

### Prerequisites
- **Docker** and **Docker Compose**
- **Python** (>=3.8)
- **Internet Connection**

### Steps

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/Ravindrasaragadam/SentimentStream.git
    cd SentimentStream
    ```
2. **Run the Setup Script**:
    ```bash
    ./setup.sh
    ```
This should spin the required containers to run and view Airflow pipelines

3. **Build Docker Image and Start Services** (Alternative to 2, Manual step to start the build):
    ```bash
    docker-compose up --build
    ```

## Configuration

1. **Configure Spark and Storage**:
   - Modify configuration files in `configs/` as needed:
     - `source_config.ini`: Source and keyword detils of the pipelines
     - `paths_config.ini`: Input & output path details of data files in pipelines

2. **Environment Variables**:
   - Store sensitive data in `.env` files (API keys, database credentials) for security.

## Usage

- **Airflow Web UI**: Access Airflow at `http://localhost:8080` to trigger and monitor DAGs.
- **Run Pipelines**: Pipelines can be started manually through the Airflow UI.
- **Check Results/Output**: Access Postgres at `http://localhost:5432` from GUI/CLI tools.

### Environment Variables
Store sensitive data in `.env` files (e.g., database credentials).

## Database Tables Created by Pipelines

### Pipeline 1: Fetch Article

1. **Table: `raw_articles`**
   - **Description**: Stores raw article data fetched from external APIs.
   - **Columns**:
     - `id`: Unique ID for the article (Primary Key)
     - `title`: Title of the article
     - `link`: Link to the original article
     - `description`: Brief description of the article
     - `date`: Date the article was published
     - `keyword`: Keyword associated with the article
     - `sentiment_score`: Calculated sentiment score for the article

### Pipeline 2: Movie Sentiment and Recommendation Analysis

1. **Table: `similar_movies`**
   - **Description**: Stores recommended movies based on similarity scores.
   - **Columns**:
     - `id`: Unique ID (Primary Key)
     - `movie_id`: ID of the original movie
     - `movie_title`: Title of the original movie
     - `similar_movie_id`: ID of the recommended similar movie
     - `similar_movie_title`: Title of the recommended similar movie
     - `similarity_score`: Content-based similarity score
     - `cooccurrence_strength`: Strength of co-occurrence with the original movie

2. **Table: `mean_age`**
   - **Description**: Stores average age data for each occupation group.
   - **Columns**:
     - `id`: Unique ID (Primary Key)
     - `occupation`: Occupation category
     - `mean_age`: Mean age of individuals in the occupation group

3. **Table: `top_rated_movies`**
   - **Description**: Stores top-rated movies based on average ratings.
   - **Columns**:
     - `id`: Unique ID (Primary Key)
     - `movie_id`: Unique ID for the movie
     - `title`: Title of the movie
     - `avg_rating`: Average rating of the movie
     - `rating_count`: Total number of ratings received

4. **Table: `top_genres`**
   - **Description**: Stores popular genres based on user demographics.
   - **Columns**:
     - `id`: Unique ID (Primary Key)
     - `age_group`: Age group of users
     - `occupation`: Occupation category of users
     - `top_genres`: List of top genres for the demographic group

---

## Adding Screenshots

To illustrate the DAG workflow or table contents, add screenshots to the `README.md` as shown below:

### Pipeline DAGs
   ![DAG Screenshot](evidences/image.png)
   ![DAG Screenshot](evidences/image1.png)

### Database Tables
   ![Database Screenshot](evidences/image3.png)
   ![Database Config](evidences/image4.png)
