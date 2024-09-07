import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2

# Function to create a Kafka consumer for a specific topic
def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Fetch voting statistics from PostgreSQL (cached for performance)
@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting_db user=postgres password=postgres")
    cur = conn.cursor()

    # Query for total voters count
    cur.execute("SELECT count(*) FROM voters")
    voters_count = cur.fetchone()[0]

    # Query for total candidates count
    cur.execute("SELECT count(*) FROM candidates")
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count

# Poll data from Kafka consumer
def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

# Function to plot a bar chart for vote counts per candidate
def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Counts per Candidate')
    plt.xticks(rotation=90)
    return plt

# Function to plot a donut chart for vote distribution
def plot_donut_chart(data: pd.DataFrame, title='Vote Distribution', type='candidate'):
    labels = list(data['candidate_name']) if type == 'candidate' else list(data['gender'])
    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title(title)
    return fig

# Function to paginate data display
def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True)
    if sort == "Yes":
        sort_field = st.selectbox("Sort By", options=table_data.columns)
        sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(by=sort_field, ascending=sort_direction == "⬆️")
    batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    current_page = st.number_input("Page", min_value=1, max_value=(len(table_data) // batch_size) or 1)
    pagination_data = table_data.iloc[(current_page - 1) * batch_size:current_page * batch_size]
    st.dataframe(pagination_data, use_container_width=True)

# Function to update the dashboard data
def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch and display voter statistics
    voters_count, candidates_count = fetch_voting_stats()
    st.metric("Total Voters", voters_count)
    st.metric("Total Candidates", candidates_count)

    # Kafka data for candidate votes
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    # Display leading candidate info
    leading_candidate = results.loc[results['total_votes'].idxmax()]
    st.image(leading_candidate['photo_url'], width=200)
    st.subheader(f"{leading_candidate['candidate_name']} - {leading_candidate['party_affiliation']}")
    st.write(f"Total Votes: {leading_candidate['total_votes']}")

    # Plot and display bar chart and donut chart
    col1, col2 = st.columns(2)
    col1.pyplot(plot_colored_bar_chart(results))
    col2.pyplot(plot_donut_chart(results))

    # Paginate candidate data table
    paginate_table(results)

# Streamlit app structure
def main():
    st.title('Election Dashboard')
    st.header("Real-time Voting Results and Analysis")

    # Trigger periodic refresh every 15 seconds
    st_autorefresh(interval=15 * 1000, key="dashboard_refresh")
    
    update_data()

# Entry point for the Streamlit app
if __name__ == '__main__':
    main()
