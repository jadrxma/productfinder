# streamlit_app.py

import streamlit as st
import requests
import pandas as pd
import time

def fetch_products(base_url, page=1, timeout=10):
    try:
        # Construct the URL with pagination and add a timeout for each page
        url = f"{base_url}/products.json?page={page}"
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        return data.get("products", [])
    except requests.exceptions.Timeout:
        st.warning(f"Request for page {page} timed out for {base_url}. Skipping.")
        return []  # Return an empty list if the request times out
    except requests.RequestException as e:
        st.error(f"An error occurred with {base_url}: {e}")
        return []

def collect_all_products(base_url, max_total_time=35):
    products = []
    page = 1
    start_time = time.time()
    
    while True:
        # Check if total time spent exceeds max_total_time
        if time.time() - start_time > max_total_time:
            st.warning(f"Processing {base_url} took too long. Skipping this URL.")
            return []  # Return an empty list if the total time exceeds the limit
        
        # Fetch products from the current page
        page_products = fetch_products(base_url, page=page)
        if not page_products:
            break  # Stop if no more products are found or request fails
        products.extend(page_products)
        page += 1
    
    # Add source URL to each product
    for product in products:
        product['source_url'] = base_url
    return products

def process_batch(batch, batch_num, progress_bar, progress_text):
    batch_data = []
    total_urls = len(batch)
    for i, base_url in enumerate(batch, start=1):
        # Start timing the request
        start_time = time.time()
        
        # Display the current URL being processed
        st.write(f"Processing URL {i}/{total_urls} in Batch {batch_num}: {base_url}")
        
        products = collect_all_products(base_url)
        if products:
            batch_data.extend(products)
        
        # Calculate the elapsed time for this URL
        elapsed_time = time.time() - start_time
        
        # Update the progress bar and real-time progress text with elapsed time
        progress_bar.progress(i / total_urls)
        progress_text.write(f"Processing {i}/{total_urls} URLs in Batch {batch_num} - Elapsed Time: {elapsed_time:.2f} seconds")
        
    return batch_data

# Streamlit App UI
st.title("E-commerce Product Collector with Batching, Timeout, URL Display, and Real-Time Timer")

# File uploader for CSV with URLs
uploaded_file = st.file_uploader("Upload a CSV file with a 'link' column for the base URLs", type="csv")

if uploaded_file:
    # Read the CSV file
    csv_data = pd.read_csv(uploaded_file)
    if 'link' not in csv_data.columns:
        st.error("The CSV file must contain a 'link' column with base URLs.")
    else:
        links = csv_data['link'].tolist()
        
        # Divide links into 4 batches
        num_batches = 4
        batch_size = len(links) // num_batches
        batches = [links[i * batch_size:(i + 1) * batch_size] for i in range(num_batches)]

        if st.button("Collect Products from All Batches"):
            all_results = []
            for batch_num, batch in enumerate(batches, 1):
                with st.spinner(f"Collecting products for Batch {batch_num}..."):
                    # Initialize the progress bar and text for the current batch
                    progress_bar = st.progress(0)
                    progress_text = st.empty()
                    
                    # Process the batch and update progress
                    batch_data = process_batch(batch, batch_num, progress_bar, progress_text)
                    all_results.extend(batch_data)
                    
                    # Display and download each batch as it completes
                    if batch_data:
                        st.success(f"Batch {batch_num} completed with {len(batch_data)} products.")
                        batch_df = pd.DataFrame(batch_data)
                        st.dataframe(batch_df)

                        # Download batch as CSV
                        csv = batch_df.to_csv(index=False)
                        st.download_button(
                            label=f"Download Batch {batch_num} Products CSV",
                            data=csv,
                            file_name=f"batch_{batch_num}_products.csv",
                            mime="text/csv"
                        )

            # Final combined download for all batches
            if all_results:
                all_df = pd.DataFrame(all_results)
                csv = all_df.to_csv(index=False)
                st.download_button(
                    label="Download All Products CSV",
                    data=csv,
                    file_name="all_products.csv",
                    mime="text/csv"
                )
