import streamlit as st
import polars as pl
import numpy as np
import duckdb
import plotly.express as px

from datetime import date

#NOTE PLEASE ENSURE NOTEBOOK IS RAN BEFORE THE STREAMLIT APP


#Configure the settings for the page
st.set_page_config(
    page_title='NYC Taxi Dashboard',
    page_icon='🚕',
    layout='wide',
    initial_sidebar_state='expanded'
)

#A title for the dashboard
st.title("🚕 NYC Yellow Taxi Trip Dashboard (January 2024)")

#This is a brief description of what the dashboard does
st.subheader("Dashboard Overview")
st.markdown(""" 
    This dashboard gives key insights into NYC Yellow Taxi trends for January 2024 with patterns being found in 
    common pickup zones, average fares and the distances of trips for the month. The filters in this dashboard allows
    you to explore trip behaviour by date, time and payment type as well.
""")

#Below is the load function that used streamlit's cache_data decorator
@st.cache_data
def load_data():
    yellow_taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    taxi_zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    try:
        
        taxi_trip_df = pl.read_parquet(yellow_taxi_url)
    except FileNotFoundError:
        st.error("Can't find the cleaned data, please run the notebook first to get cleaned data")
        st.stop()
    
    try:
        taxi_zone_df = pl.read_csv(taxi_zone_url)
    except FileNotFoundError:
        st.error("Can't find the taxi zone data, please run the notebook first to download the csv file for the zones")
        st.stop()

    return taxi_trip_df, taxi_zone_df

#We then load the cleaned taxi trip data into a polars dataframe along with the base zone data
taxi_trip_df, taxi_zone_df = load_data()
        
#------------------------------
# Below we clean the data again before using it
#------------------------------

#We will now sanitize the data for preparation for analysis

#First we clean up any rows with null values in important columns such as pick and dropoff times, locations, fares and trips

important_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "trip_distance"
]

taxi_trip_df = taxi_trip_df.drop_nulls(subset=important_columns)

#We now clean the data by removing any rows where the trip has zero or negative distance, negative fares, or fares exceeding $500

taxi_trip_df = taxi_trip_df.filter(
    pl.col("trip_distance") > 0,
    pl.col("fare_amount") >= 0,
    pl.col("fare_amount") <= 500
)

#For our last bit of sanitization we then remove rows where dropoff time is before pickup time

taxi_trip_df = taxi_trip_df.filter(
    pl.col("tpep_dropoff_datetime") >= pl.col("tpep_pickup_datetime")
)


#Next we do feature engineering to add our own derived columns to the dataset

#Adding trip duration in minutes
taxi_trip_df = taxi_trip_df.with_columns([
    ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
     .dt.total_seconds() / 60)
     .alias("trip_duration_minutes")
])

#Adding trip speed in mph
taxi_trip_df = taxi_trip_df.with_columns([
    pl.when(pl.col("trip_duration_minutes") > 0)
      .then(pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60))
      .otherwise(None)
      .alias("trip_speed_mph")
])

#Adding pickup hour
taxi_trip_df = taxi_trip_df.with_columns([
    pl.col('tpep_pickup_datetime').dt.hour().alias('pickup_hour')
])

#Adding pickup day of week
taxi_trip_df = taxi_trip_df.with_columns([
    pl.col('tpep_pickup_datetime').dt.strftime("%A").alias('pickup_day_of_week')
])


# -----------------------------
# Below we set up the interactive filters
# -----------------------------         
st.sidebar.header("Interactive Filters")

# Date range filter
min_date = date(2024, 1, 1)
max_date = date(2024, 2, 1)

date_range = st.sidebar.date_input(
    "Select Pickup Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Hour range slider (0-23)
hour_range = st.sidebar.slider(
    "Select Pickup Hour Range",
    min_value=0,
    max_value=23,
    value=(0, 23)
)

# Payment type multiselect

#A map for the payment options
payment_type_map = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    0: "Other"
}

payment_type_codes = sorted(taxi_trip_df["payment_type"].unique().to_list())

payment_type_labels = [payment_type_map.get(code, "Unkown") 
                       for code in payment_type_codes]


payment_type_filter = st.sidebar.multiselect(
    "Select Payment Types",
    options=payment_type_labels,
    default=payment_type_labels
)

#Needed to convert back to code for filtering
label_to_code = {v: k for k, v in payment_type_map.items()}

selected_payment_codes = [label_to_code[label] for label in payment_type_filter]

#Check if date_range is valid
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    st.warning("Please select a start and end date for the date range.")
    st.stop()

#Check if a payment type is selected
if len(selected_payment_codes) == 0:
    st.warning("No data to display. Please select at least one payment type.")
    st.stop()

#Applying the filters to the dataframe
filtered_trips = taxi_trip_df.filter(
    (pl.col("tpep_pickup_datetime").dt.date().is_between(start_date, end_date)) &
    (pl.col("pickup_hour").is_between(hour_range[0], hour_range[1])) &
    (pl.col("payment_type").is_in(selected_payment_codes))
)

#Check if the filter is valid
if filtered_trips.height == 0:
    st.warning("No trips match the selected filters. Please widen your filter selection.")
    st.stop()

#Below is the code for the Key Metrics
st.subheader("Key Metrics")
    

#Get the data from the cleaned taxi trip data frame
total_trips = filtered_trips.height
avg_fare = filtered_trips.get_column("fare_amount").mean()
total_revenue = filtered_trips.get_column("total_amount").sum()
avg_trip_distace = filtered_trips.get_column("trip_distance").mean()
avg_duration = filtered_trips.get_column("trip_duration_minutes").mean()

#Set the columns up for the metrics
col1, col2, col3, col4, col5 = st.columns(5)

#Display the metrics
col1.metric("Total Trips", f"{total_trips:,}")
col2.metric("Average Fare ($)", f"{avg_fare:.2f}")
col3.metric("Total Revenue ($)", f"{total_revenue:,.2f}")
col4.metric("Average Trip Distance (miles)", f"{avg_trip_distace:.2f}")
col5.metric("Average Trip Duration (minutes)", f"{avg_duration:.2f}")


#Below is the code for displayed the visuals
st.subheader("Dashboard Overview")

#Set the tabs for the different visuals displayed
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Top 10 Pickup Zones", 
     "Average Fare by Hour", 
     "Distribution of Trip Distances", 
     "Breakdown of Payment Types", 
     "Trips by Day of Week and Hour"
    ]
)

#Setting up DuckDb to do our SQL queries for the visuals

# Create a DuckDB connection
con = duckdb.connect()

#Register dataframes
con.register("taxi_trips", filtered_trips)
con.register("taxi_zones", taxi_zone_df)


# -----------------------------
# Top 10 Pickup Zones
# -----------------------------

with tab1:
    #SQL query from notebook for finding busiest pickup zones
    busiest_pickup_zones = con.execute('''
        SELECT 
            z."Zone" AS pickup_zone,
            COUNT(*) AS total_trips
        FROM
            taxi_trips t
        JOIN
            taxi_zones z 
                ON t.PULocationID = z.LocationID
        GROUP BY 
            z."Zone"
        ORDER BY 
            total_trips DESC
        LIMIT 10
                                    
    '''
    ).fetchdf()

    #Chart configs
    pickup_zone_bar_chart = px.bar(
        busiest_pickup_zones,
        x="total_trips",
        y="pickup_zone",
        orientation='h', 
        title="Top 10 Pickup Zones by Trip Count",
        color= "total_trips",
        color_continuous_scale='Plasma',
        labels={
            "pickup_zone" : "Pickup Zone",
            "total_trips" : "Total Trips"
        }
    )

    pickup_zone_bar_chart.update_layout(
        height=600, 
        yaxis={'categoryorder': 'total ascending'}, 
        title_x=0.5,  
        title_font_size=16,
    )

    st.plotly_chart(pickup_zone_bar_chart, use_container_width=True)

    st.markdown("""
**Insight:** The dominating pickup zones being Midtown Center and Upper East Side South and North indicate that there is a concentration of taxi trips mainly around these commercial/business areas as people make commutes from these central hubs to other areas of NYC. Furthermore JFK Airport being another hotspot indicates many tourists use the taxi service to reach other areas in the NYC area. This tells us there is high population movement and business activity within these areas.
""")

# -----------------------------
# Average Fare by Hour of Day
# -----------------------------

with tab2:
    #SQL query
    average_fair = con.execute(''' 
        SELECT 
            pickup_hour,
            ROUND(AVG(fare_amount), 2) as avg_fare_amount
        FROM
            taxi_trips
        GROUP BY 
        pickup_hour
        ORDER BY 
            pickup_hour
    ''').fetchdf()

    #Chart configs
    avg_fare_line_chart = px.line(
        average_fair,
        x = "pickup_hour",
        y = "avg_fare_amount",
        title="Average Fare by Hour of Day",
        markers=True,
        labels={
            "pickup_hour": "Hour of Day",
            "avg_fare_amount": "Average Fare"
        },
        line_shape='spline'
    )


    avg_fare_line_chart.update_layout(
        height=600, 
        title_x=0.5,  
        title_font_size=18,
        hovermode='x unified'
    )

    hour_labels = {h: f"{h%12 or 12}{'am' if h<12 else 'pm'}" for h in range(24)}
    avg_fare_line_chart.update_xaxes(
        tickvals=list(range(24)),
        ticktext=[hour_labels[h] for h in range(24)],
        tickangle=45,
        range=[-0.5, 23.5] 
    )


    avg_fare_line_chart.update_yaxes(
        tickprefix="$",
        tickformat=".2f",
    )


    st.plotly_chart(avg_fare_line_chart, use_container_width=True)
    st.markdown("""
**Insight:** The spike in the average fare during the hours of 3am-7am shows that the fare goes up during the early hours of the morning when most people commute to work. The spike in the average fare may then be due to the trips being longer for these commuters to their place of work across NYC or due to traffic congestion created during the peak hours of the morning.
""")
    
# -----------------------------
# Distribution of Trip Distances
# ----------------------------- 

with tab3:
    # Polars data filtering
    filtered_distances = filtered_trips.filter(pl.col("trip_distance") < 50)
    median_distance = filtered_distances.get_column("trip_distance").median()

    # Chart Config  
    trip_dist_histogram = px.histogram(
        filtered_distances,
        x="trip_distance",
        nbins= 100,
        title="Distribution of Trip Distances",
        opacity=0.7,
        color_discrete_sequence=['steelblue'],
    )

    trip_dist_histogram.update_xaxes(
        range=[0, filtered_distances['trip_distance'].max()],  
        title="Trip Distance (miles)",
        dtick=5,
        tickformat=".1f",
    )

    trip_dist_histogram.update_yaxes(
        title="Number of Trips",
    )

    trip_dist_histogram.update_layout(
        height=600,
        title_x=0.5,  
        title_font_size=18,
        font_size=12,
        bargap=0.05,
        hoverlabel=dict(
            font_size=12,
            font_family="Arial"
        )
    )

    trip_dist_histogram.add_vline(
    x=median_distance,
    line_width=3,
    line_dash="dash",
    line_color="red",
    annotation_text=f"Median: {median_distance:.1f} miles",
    annotation_position="top right",
    annotation_font_size=14,
    annotation_font_color="red",
    annotation_bgcolor="white",
    annotation_bordercolor="red",
    annotation_borderwidth=1
    )

    trip_dist_histogram.update_traces(
        hovertemplate="<b>Trip Distance</b>: %{x:.1f} miles<br>" +
                    "<b>Count</b>: %{y:,} trips<br>",
        marker_line_color='white',
        marker_line_width=0.5
    )


    st.plotly_chart(trip_dist_histogram, use_container_width=True)

    st.markdown("""
    **Insight:** Most taxi trips are short distance with the median being 1.7 miles. This shows us that the taxi service is mainly used for transport locally within areas of the city with the majority of trips not being long distance outside of these local areas.
    """)


# -----------------------------
# Percentage of Payment Type for Trips
# -----------------------------

with tab4:
    #SQL query 
    payment_type_perc = con.execute(''' 
        SELECT 
            payment_type,        
            CASE payment_type
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                ELSE 'Other'
            END AS payment_method,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_trips
        FROM
            taxi_trips
        GROUP BY 
            payment_type
        ORDER BY
            payment_type                    
    ''').fetchdf()

    #Chart Config
    payment_type_bar_chart = px.bar(
        payment_type_perc,
        x = "percentage_of_trips",
        y = "payment_method",
        orientation='h', 
        title="Payment Type Breakdown",
        color= "percentage_of_trips",
        color_continuous_scale='Viridis',
        labels={
            "payment_method" : "Payment Method",
            "percentage_of_trips" : "Percentage of Trips"
        },
    )

    payment_type_bar_chart.update_layout(
        height=600, 
        yaxis={'categoryorder': 'total ascending'}, 
        title_x=0.5,  
        title_font_size=16,
    )

    payment_type_bar_chart.update_xaxes(
        dtick = 5,
        ticksuffix="%"
    )

    st.plotly_chart(payment_type_bar_chart, use_container_width=True)
    st.markdown("""
    **Insight:** The majority of trips being paid by credit card indicate that passengers prefer convenience during payment and traceability to their transaction when using the taxi service. Cash only making up 14.74% of payments for trips tell us that the majority of passengers find cash to be too cumbersome and time consuming during payment.
    """)

# -----------------------------
# Trips by Day of Week and Hour
# -----------------------------

with tab5:
    # Create pivot table for heatmap
    # We convert it to a pandas df then group by the two required columns then counts the trips and coverts to wide format
    heatmap_data = (
    filtered_trips
    .to_pandas()
    .groupby(['pickup_day_of_week', 'pickup_hour'])
    .size()
    .unstack(fill_value=0)
    )

    # Rename weekdays for clarity
    weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex(weekday_names)

    def hour_to_ampm(hour):
        return f"{hour%12 or 12}{'am' if hour < 12 else 'pm'}"
    
    hour_cols = list(heatmap_data.columns)
    hour_labels = [hour_to_ampm(hour) for hour in hour_cols]

    #Chart Config
    day_hour_heatmap = px.imshow(
        heatmap_data,
        labels=dict(x='Hour of Day', y='Day of Week', color='Trip Count'),
        x=hour_labels,
        y=weekday_names,
        color_continuous_scale='YlOrRd',
        title='Taxi Trip Volume: Hour of Day vs Day of Week'
    )

    day_hour_heatmap.update_layout(height=500, title_x=0.5)

    day_hour_heatmap.update_xaxes(
        tickangle=45,
        range=[-0.5, 23.5] 
    )

    st.plotly_chart(day_hour_heatmap, use_container_width=True)
    st.markdown("""
**Insight:** Weekday mornings show notable spikes in taxi trips as people make commutes to work while late afternoons show strong peaks as people go about doing business, commuting from work or other leisure activities. The weekends on the other hand have a more spread out pattern with Saturdays still haivng a notable spike in the afternoon with this also being attributed to leisure activities.
""")
