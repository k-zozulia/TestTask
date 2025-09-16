# ETL Data Pipeline Project

## Overview
This project is a Python-based **ETL (Extract, Transform, Load) pipeline** that fetches data from a public REST API, processes and enriches it, loads it into a SQLite database, and generates analytical reports in **JSON** and **CSV** formats.  

The pipeline is modular, with separate components for data extraction, transformation, loading, and analytics, and supports running individual stages or the full pipeline.

---

## API Used
**URL:** [https://jsonplaceholder.typicode.com](https://jsonplaceholder.typicode.com)

**Endpoints:**
- `/users`: Retrieves user data (e.g., ID, username, email, address, company).
- `/posts`: Retrieves post data (e.g., post ID, user ID, title, body).

---

## Selected and Processed Fields

### Users
**Raw Fields (from API):**
id, username, name, email, phone, website, address.city, address.suite, address.street, address.zipcode, address.geo.lat, address.geo.lng,
company.name, company.catchPhrase, company.bs

**Processed Fields (in Parquet and SQLite):**
- `user_id`: Integer (from id)  
- `username`: String (max length 100)  
- `name`: String (max length 200)  
- `email`: String, converted to lowercase (max length 200)  
- `phone`: String (max length 50)  
- `website`: String (max length 200)  
- `city`: Extracted from address.city (max length 100)  
- `zipcode`: Extracted from address.zipcode (max length 20)  
- `lat`: Float, from address.geo.lat  
- `lng`: Float, from address.geo.lng  
- `company_name`: Extracted from company.name (max length 200)  
- `company_catchphrase`: Text, from company.catchPhrase  
- `email_domain`: Extracted from email (e.g., example.com, max length 100)  
- `has_coordinates`: Boolean, true if both lat and lng are present  
- `created_at`: Datetime, timestamp of data processing  

**Transformations:**
- Flattened nested fields (`address`, `company`)  
- Converted email to lowercase and extracted domain  
- Replaced missing values with empty strings  
- Converted lat/lng to numeric with validation  

---

### Posts
**Raw Fields (from API):**
id, userId, title, body

**Processed Fields (in Parquet and SQLite):**
- `post_id`: Integer (from id)  
- `user_id`: Integer (from userId)  
- `title`: String, stripped of whitespace (max length 500)  
- `body`: Text, stripped of whitespace  
- `title_length`: Integer (length of title)  
- `body_length`: Integer (length of body)  
- `word_count`: Integer (number of words in body)  
- `post_category`: String, categorized as:
  - short (<100 chars)  
  - medium (100–200 chars)  
  - long (>200 chars)  
- `created_at`: Datetime, timestamp of data processing  

**Transformations:**
- Stripped whitespace from title and body  
- Added calculated fields (`title_length`, `body_length`, `word_count`, `post_category`)  


---

## Prerequisites
- **Python:** 3.10 or higher  
- **Dependencies:** (in `requirements.txt`)


## Setup Instructions

### Clone the repository
```bash
git clone https://github.com/k-zozulia/TestTask.git
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Create project structure (optional)
```bash
python main.py --setup
```

## Running the Pipeline

The pipeline can be run in full or by individual stages (extract, transform, load, analytics).

### Full Pipeline
```bash
python main.py --stage full --api-url https://jsonplaceholder.typicode.com --db-path local.db --data-dir data
```
### Individual Stages

#### Extract: Fetches data from API and saves to data/raw/yyyy-mm-dd/.

```bash
python main.py --stage extract
```

#### Transform: Processes raw data and saves to data/processed/yyyy-mm-dd/.

```bash
python main.py --stage transform
```

#### Load: Loads processed data into SQLite (local.db).

```bash
python main.py --stage load
```

#### Analytics: Runs SQL queries and generates reports in reports/.
```bash
python main.py --stage analytics
```

## Command-Line Arguments

- `--stage`: Pipeline stage to run (`extract`, `transform`, `load`, `analytics`, `full`). Default: `full`.  
- `--api-url`: API base URL. Default: `https://jsonplaceholder.typicode.com`.  
- `--db-path`: Path to SQLite database. Default: `local.db`.  
- `--data-dir`: Directory for data storage. Default: `data`.  
- `--setup`: Create project directory structure (no pipeline execution).  

---
## Output

- **Raw Data:** JSON files in `data/raw/yyyy-mm-dd/`  
  (e.g., `users.json`, `posts.json`)  

- **Processed Data:** Parquet files in `data/processed/yyyy-mm-dd/`  
  (e.g., `users.parquet`, `posts.parquet`)  

- **Database:** SQLite database (`local.db`) with tables `users` and `posts`  

- **Reports:**
  - **JSON:** `reports/summary_report_YYYYMMDD_HHMMSS.json` with analytics results  
  - **CSV:** Individual query results (e.g., `reports/user_statistics_YYYYMMDD_HHMMSS.csv`)  

- **Logs:** Pipeline execution logs in `logs/pipeline.log`  

---

## Analytics Queries
The pipeline generates three SQL queries:

1. **User Statistics**
   - Total users  
   - Unique email domains  
   - Users with coordinates  
   - Users with company names  

2. **Post Statistics**
   - Total posts  
   - Unique authors  
   - Average/max/min title and body lengths  
   - Average word count  

3. **User Post Activity**
   - Post count per user  
   - Average/max post length per user  
   - Grouped by user  

