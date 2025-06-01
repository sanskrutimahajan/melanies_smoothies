# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests


# Get Snowflake session from Streamlit connection
cnx = st.connection("snowflake", type="snowflake")
session = cnx.session()  # ✅ CALL the function to get the session object

# Title
st.title("🥤 Smoothie Order Dashboard")

# Section: Smoothie Selection
st.header("🧃 Place a Smoothie Order")

# Load smoothies from Snowflake
smoothies_df = session.table("smoothies.public.smoothies").to_pandas()
smoothies_df.columns = smoothies_df.columns.str.lower()
smoothie_options = smoothies_df["name"].tolist()

selected_smoothie = st.selectbox("Choose your smoothie:", smoothie_options)

# Load customers from Snowflake
customer_df = session.table("smoothies.public.customers").to_pandas()
customer_df.columns = customer_df.columns.str.lower()
customer_options = customer_df["name"].tolist()

selected_customer = st.selectbox("Select your name:", customer_options)

# Get selected IDs safely
smoothie_row = smoothies_df[smoothies_df["name"] == selected_smoothie]
customer_row = customer_df[customer_df["name"] == selected_customer]

if not smoothie_row.empty and not customer_row.empty:
    smoothie_id = smoothie_row["smoothie_id"].values[0]
    customer_id = customer_row["customer_id"].values[0]

    if st.button("Place Order"):
        session.sql(f"""
            INSERT INTO smoothies.public.orders (order_id, customer_id, smoothie_id, order_time, status)
            SELECT COALESCE(MAX(order_id), 0) + 1, {customer_id}, {smoothie_id}, CURRENT_TIMESTAMP, 'pending'
            FROM smoothies.public.orders;
        """).collect()
        st.success(f"✅ Order placed for **{selected_smoothie}** by **{selected_customer}**!")

# Section: Order Summary
st.header("📦 Recent Orders")

orders_df = (
    session.table("smoothies.public.orders")
    .sort(col("order_time").desc())
    .limit(10)
    .to_pandas()
)
orders_df.columns = orders_df.columns.str.lower()

# Merge smoothie and customer info
orders_df = (
    orders_df
    .merge(smoothies_df[["smoothie_id", "name"]].rename(columns={"name": "Smoothie"}), on="smoothie_id", how="left")
    .merge(customer_df[["customer_id", "name"]].rename(columns={"name": "Customer"}), on="customer_id", how="left")
)

# Show recent orders
st.dataframe(orders_df[["order_id", "Customer", "Smoothie", "order_time", "status"]])

#New section to display smoothiefroot nutirtion information
smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
st.text(smoothiefroot_response)
