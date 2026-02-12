# 🚕 NYC Yellow Taxi Dashboard — COMP3610 Assignment 1

## Overview

This project analyzes and visualizes NYC Yellow Taxi trip data for January 2024 using a data processing notebook and an interactive Streamlit dashboard.

The workflow consists of:

- **A Jupyter Notebook** for data cleaning, feature engineering, and exporting a cleaned dataset.
- **A Streamlit app** (`app.py`) for interactive filtering, key metrics, and required visualizations.

The dashboard allows users to explore taxi trip behavior by:

- Date range
- Hour of day
- Payment type

It displays key performance metrics and five required visualizations using Plotly.

## Project Structure

```
.
├── assignment1.ipynb                # Data cleaning & feature engineering notebook
├── app.py                           # Streamlit dashboard application
├── cleaned_engineered_taxi_data.parquet
├── data/
│   └── raw/
│       └── taxi_zone_lookup.csv
├── requirements.txt
└── README.md

```

## Setup Instructions

### Prerequisites

- Python 3.1 or higher
- pip package manager
- Git (optional)

### Installation

1. **Clone the repository** (if applicable)

   ```bash
   git clone <repository-url>
   cd '.\COMP 3610 Assignment 1\
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment**
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Project

1. **Run The Notebook**

```bash
assignment1.ipynb
```

2. **Run The Dashboard**

```bash
streamlit run app.py
```
