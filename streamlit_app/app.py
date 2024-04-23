import streamlit as st

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta
import pandas as pd
import time
import psycopg2
import matplotlib.pyplot as plt
import plotly.express as px
import pycountry
from collections import Counter
import json

from datetime import datetime, timedelta
from datetime import datetime, timedelta, time as dt_time  



endpoint = "cb.u5tbreifenk4gngi.cloud.couchbase.com"
username = "DBMS"
password = "Dbms@123"
bucket_name = "dbmsProject"
scope_name = "twitter_new"
collection_name = "tweet_data"

PGHOST = "twitter.postgres.database.azure.com"
PGUSER = "neeraj"
PGPORT = 5432
PGDATABASE = "postgres"
PGPASSWORD = "Dbms@123"

authenticator = PasswordAuthenticator(username, password)
timeout_opts = ClusterTimeoutOptions(connect_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))
options = ClusterOptions(authenticator=authenticator, timeout_options=timeout_opts)
cluster = Cluster(f'couchbases://{endpoint}', options)
cluster.wait_until_ready(timedelta(seconds=5))
bucket = cluster.bucket(bucket_name)
inventory_scope = bucket.scope(scope_name)
cb_coll = inventory_scope.collection(collection_name)

st.title("TwitSeeker: Search Engine for Twitter")
search_type = st.selectbox(
    "Search by:",
    ("Tweets", "Hashtag", "Username"),
    index=0  
)

query = st.text_input("Enter your search query here...")

def search_by_hashtag(hashtag, start_datetime, end_datetime, sort_by, bucket_name, scope_name, collection_name):
    start_time = time.time()
    hashtag_quoted = f'"{hashtag}"'
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    order_by = sort_options[sort_by]

    sql_query = f"""
    SELECT tweet_data.created_at, tweet_data.favorite_count, tweet_data.hashtags, tweet_data.id,
           tweet_data.is_retweet, tweet_data.original_tweet_id, tweet_data.reply_count,
           tweet_data.retweet_count, tweet_data.retweeted_status, tweet_data.text, tweet_data.urls, tweet_data.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE ANY h IN SPLIT(SUBSTR(hashtags, 2, LENGTH(hashtags) - 2), "', '") SATISFIES h = {hashtag_quoted} END
    AND tweet_data.created_at BETWEEN '{start_datetime_str}' AND '{end_datetime_str}'
    ORDER BY {order_by};
    """
    try:
        row_iter = inventory_scope.query(sql_query)
        results = [row for row in row_iter]
        df = pd.DataFrame(results)
        elapsed_time = time.time() - start_time
        total_count = len(df)
        return df, total_count, elapsed_time
    except Exception as e:
        print("Error during database query: " + str(e))
        return pd.DataFrame(), 0, 0

def search_by_text(search_text, start_datetime, end_datetime, sort_by, bucket_name, scope_name, collection_name):
    start_time = time.time()
    like_pattern = f"%{search_text}%"
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    order_by = sort_options[sort_by] 

    sql_query = f"""
    SELECT tweet_data.created_at, tweet_data.favorite_count, tweet_data.hashtags, tweet_data.id,
           tweet_data.is_retweet, tweet_data.original_tweet_id, tweet_data.reply_count,
           tweet_data.retweet_count, tweet_data.retweeted_status, tweet_data.text, tweet_data.urls, tweet_data.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweet_data.text LIKE '{like_pattern}'
    AND tweet_data.created_at BETWEEN '{start_datetime_str}' AND '{end_datetime_str}'
    ORDER BY {order_by};
    """

    try:
        row_iter = inventory_scope.query(sql_query)
        results = []
        if row_iter:
            results = [row for row in row_iter]
        df = pd.DataFrame(results)

        elapsed_time = time.time() - start_time
        total_count = len(df)
        return df, total_count, elapsed_time
    except Exception as e:
        print("Error during database query: " + str(e))
        return pd.DataFrame(), 0, 0



def search_by_username(username):
    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    cur = conn.cursor()

    cur.execute("SELECT id, name, screen_name, location, url, followers_count, friends_count, statuses_count, verified, created_at FROM users_data WHERE name = %s", (username,))
    user_details = cur.fetchone()
    
    if not user_details:
        cur.close()
        conn.close()
        return "User not found.", pd.DataFrame(), 0  
    
    user_data = {
        "ID": user_details[0],
        "Name": user_details[1],
        "Screen Name": user_details[2],
        "Location": user_details[3],
        "URL": user_details[4],
        "Followers Count": user_details[5],
        "Friends Count": user_details[6],
        "Statuses Count": user_details[7],
        "Verified": user_details[8],
        "Created At": user_details[9],
    }
    
    start_time = time.time()
    tweet_query = f"""
    SELECT tweet_data.created_at, tweet_data.favorite_count, tweet_data.hashtags, tweet_data.id,
           tweet_data.is_retweet, tweet_data.original_tweet_id, tweet_data.reply_count,
           tweet_data.retweet_count, tweet_data.retweeted_status, tweet_data.text, tweet_data.urls, tweet_data.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweet_data.user_id = {user_data["ID"]};
    """
    try:
        tweet_results = inventory_scope.query(tweet_query)
        results = [row for row in tweet_results]
        df = pd.DataFrame(results)
        elapsed_time = time.time() - start_time
        cur.close()
        conn.close()
        return user_data, df, elapsed_time
    except Exception as e:
        st.error("Error during database tweet query: " + str(e))
        cur.close()
        conn.close()
        return {}, pd.DataFrame(), 0  

def get_tweets_by_user(user_id, bucket_name, scope_name, collection_name):
    sql_query = f"""
    SELECT tweet_data.created_at, tweet_data.text, tweet_data.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweet_data.user_id = {user_id}
    ORDER BY tweet_data.created_at DESC;
    """
    try:
        row_iter = inventory_scope.query(sql_query)
        results = [row for row in row_iter]
        return pd.DataFrame(results)
    except Exception as e:
        print(f"Error fetching tweets for user {user_id}: {str(e)}")
        return pd.DataFrame()
    finally:
        pass




default_start_date = datetime.now() - timedelta(days=7)
default_end_date = datetime.now()

col1, col2, col3, col4 = st.columns(4)
with col1:
    start_date = st.date_input("Start date", value=default_start_date, key="start_date_input")
with col2:
    start_time = st.time_input("Start time", value=dt_time(0, 0), key="start_time_input")
with col3:
    end_date = st.date_input("End date", value=default_end_date, key="end_date_input")
with col4:
    end_time = st.time_input("End time", value=dt_time(23, 59), key="end_time_input")

start_datetime = datetime.combine(start_date, start_time)
end_datetime = datetime.combine(end_date, end_time)

num_tweets_to_display = st.slider("Number of tweets to display:", 1, 3000, 5) 

sort_options = {
    'Most Recent': 'created_at DESC',
    'Least Recent': 'created_at ASC',
    'Most Favorited': 'favorite_count DESC',
    'Least Favorited': 'favorite_count ASC',
    'Most Replies': 'reply_count DESC',
    'Least Replies': 'reply_count ASC'
}
selected_sort = st.selectbox('Sort by:', list(sort_options.keys()))

if 'results_df' not in st.session_state:
    st.session_state.results_df = pd.DataFrame()

if st.button('Search'):
    if search_type == "Hashtag":
        results_df, total_count, elapsed_time = search_by_hashtag(query, start_datetime, end_datetime, selected_sort, bucket_name, scope_name, collection_name)
        if not results_df.empty:
            st.write(f"Total tweets found: {total_count}")
            st.write(f"Time taken to retrieve results: {elapsed_time:.2f} seconds")
            st.write("Sample Tweets:")
            st.dataframe(results_df.head(num_tweets_to_display))
        else:
            st.write("No results found.")
    elif search_type == "Tweets":
        st.session_state.results_df, total_count, elapsed_time = search_by_text(query, start_datetime, end_datetime, selected_sort, bucket_name, scope_name, collection_name)
        if not st.session_state.results_df.empty:
            st.write(f"Total tweets found: {total_count}")
            st.write(f"Time taken to retrieve results: {elapsed_time:.2f} seconds")
        else:
            st.write("No results found.")
    elif search_type == "Username":
        user_data, tweets_df, query_time = search_by_username(query)
        if isinstance(user_data, str): 
            st.write(user_data)
        else:
            st.write("User details:")
            st.json(user_data) 
            if not tweets_df.empty:
                st.write(f"Total tweets found: {len(tweets_df)}")
                st.write(f"Time taken to retrieve tweets: {query_time:.2f} seconds")
                st.write("Sample Tweets:")
                st.dataframe(tweets_df.head(num_tweets_to_display))  
            else:
                st.write("No tweets found for this user.")


if not st.session_state.results_df.empty:
    selected_indices = st.multiselect(
        "Select tweets to see more from the authors:",
        st.session_state.results_df.index,
        format_func=lambda x: f"User ID {st.session_state.results_df.loc[x, 'user_id']}"
    )
    st.dataframe(st.session_state.results_df.head(num_tweets_to_display))

    if selected_indices:
        if st.button('Show More Tweets from Selected Users'):
            for selected_index in selected_indices:
                user_id = st.session_state.results_df.loc[selected_index, 'user_id']
                user_tweets_df = get_tweets_by_user(user_id, bucket_name, scope_name, collection_name)
                if not user_tweets_df.empty:
                    st.subheader(f"More tweets by user ID {user_id}:")
                    st.dataframe(user_tweets_df)
                else:
                    st.write(f"No additional tweets found for user ID {user_id}.")
else:
    st.session_state.results_df = pd.DataFrame()
    st.write("No results found or search not yet performed.")



st.title('Dashboard Metrics')

def get_top_followed_users():
    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT name, screen_name, followers_count
        FROM users_data
        ORDER BY followers_count DESC
        LIMIT 10;
    """)
    top_users = cur.fetchall()
    cur.close()
    conn.close()
    df_top_users = pd.DataFrame(top_users, columns=["Name", "Screen Name", "Followers Count"])
    return df_top_users


with st.expander("Top 10 Most Followed Users", expanded=False):
    df_top_users = get_top_followed_users()
    st.columns(3)[1].header("Top 10 Most Followed Users")
    
    fig_most_followed = px.bar(df_top_users, y='Screen Name', x='Followers Count', orientation='h', title='Bar Chart: Most Followed Users')
    fig_most_followed.update_layout(yaxis={'categoryorder': 'total ascending'}, 
                  xaxis_title="Followers Count",
                  yaxis_title="Screen Name",
                  title={'x':0.5, 'xanchor': 'center'})
    
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_users)  
    with col2:
        st.plotly_chart(fig_most_followed) 

def get_country_code(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except:
        return None

def get_top_creator_locations():
    with psycopg2.connect(dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT location, COUNT(*) AS user_count
                FROM users_data
                WHERE location IS NOT NULL AND location != 'MISSING_INFORMATION'
                GROUP BY location
                ORDER BY user_count DESC
                LIMIT 100;
            """)
            result = cur.fetchall()
            df = pd.DataFrame(result, columns=['Location', 'User Count'])
    return df

df_top_locations = get_top_creator_locations()
df_top_locations['Country Code'] = df_top_locations['Location'].apply(get_country_code)
df_top_locations = df_top_locations[df_top_locations['Country Code'].notnull()]
df_top_locations = df_top_locations.head(10)

with st.expander("Top 10 Locations With Most Creators", expanded=True):
    st.columns(3)[1].header("Top 10 Locations With Most Creators")

    fig = px.choropleth(df_top_locations,
                        locations="Country Code",  
                        color="User Count",
                        hover_name="Location",  
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        projection="natural earth")

    fig.update_layout(
        title='Geographic Distribution of Users',
        title_x=0.325,  
        geo=dict(
            showframe=True,
            showcoastlines=True,
            projection_type='equirectangular'
        )
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.dataframe(df_top_locations)
    with col2:
        st.plotly_chart(fig, use_container_width=True)

def get_top_hashtags():
    hashtag_query = f"""
    SELECT RAW h
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    UNNEST SPLIT(SUBSTR(tweet_data.hashtags, 2, LENGTH(tweet_data.hashtags) - 2), "', '") AS h
    WHERE tweet_data.hashtags IS NOT MISSING AND tweet_data.hashtags != '[]'
    """
    
    try:
        all_hashtags_results = inventory_scope.query(hashtag_query)
        all_hashtags = [row for row in all_hashtags_results]
        
        hashtag_counter = Counter(all_hashtags)
        
        top_hashtags = hashtag_counter.most_common(10)
        
        df_top_hashtags = pd.DataFrame(top_hashtags, columns=['Hashtag', 'Count'])
        
        return df_top_hashtags
        
    except Exception as e:
        print("Error during database query:", e)
        return pd.DataFrame()

with st.expander("Top 10 Most Used Hashtags", expanded=False):
    st.columns(3)[1].header("Top 10 Most Used Hashtags")
    df_top_hashtags = get_top_hashtags()
    
    fig_most_used_hashtags = px.bar(df_top_hashtags, x='Count', y='Hashtag', orientation='h', 
                                    title='Bar Chart: Most Used Hashtags')
    fig_most_used_hashtags.update_layout(
        yaxis={'categoryorder': 'total ascending'}, 
        xaxis_title="Count",
        yaxis_title="Hashtag",
        title={'x':0.5, 'xanchor': 'center'}
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_hashtags)
    with col2:
        st.plotly_chart(fig_most_used_hashtags, use_container_width=True)

def get_top_retweeted_tweets():
    tweet_query = f"""
    SELECT original_tweet_id, COUNT(*) as retweet_count
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE is_retweet = 'True'
    GROUP BY original_tweet_id
    ORDER BY retweet_count DESC
    LIMIT 10;
    """
    tweet_results = inventory_scope.query(tweet_query)
    df_top_tweets = pd.DataFrame(tweet_results)
    return df_top_tweets

with st.expander("Top 10 Retweeted Tweets", expanded=False):
    df_top_tweets = get_top_retweeted_tweets()
    st.columns(3)[1].header("Top 10 Most Retweeted Tweets")

    
    fig = px.pie(df_top_tweets, values='retweet_count', names='original_tweet_id')

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_tweets.style.format({"retweet_count": "{:,}"}))  
    with col2:
        st.plotly_chart(fig, use_container_width=True)
