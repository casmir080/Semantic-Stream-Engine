import streamlit as st
import asyncio
import asyncpg
import pandas as pd

# Page setup for optimal screen space
st.set_page_config(
    page_title="Semantic HN Stream Engine",
    page_icon="🧠",
    layout="wide"
)

DB_DSN = "postgres://postgres:443302@localhost:5432/postgres"

async def fetch_insights():
    """Asynchronously pulls latest analytical logs from Postgres."""
    try:
        conn = await asyncpg.connect(DB_DSN)
        rows = await conn.fetch(
            "SELECT id, raw_text, semantic_summary, sentiment, subreddit, source_url, processed_at FROM insights ORDER BY processed_at DESC"
        )
        await conn.close()
        return rows
    except Exception as e:
        st.error(f"Failed to fetch data from database: {e}")
        return []

# Application Header
st.title("🧠 Semantic Stream Analysis Engine")
st.markdown("Real-time NLP evaluations and sentiments derived from live Hacker News streams.")

# Sidebar Configuration for Auto-refresh
st.sidebar.header("Dashboard Controls")
refresh_rate = st.sidebar.slider("Auto-refresh interval (seconds)", 2, 30, 5)

if st.sidebar.button("Force Refresh Data"):
    st.rerun()

# Run the async database fetch within Streamlit's sync wrapper
data_rows = asyncio.run(fetch_insights())

if not data_rows:
    st.info("Waiting for data to populate table... Keep your terminal files running to catch live Hacker News posts!")
else:
    # Convert asyncpg records directly to a Pandas DataFrame
    df = pd.DataFrame(data_rows, columns=["ID", "Raw Text", "AI Semantic Summary", "Sentiment", "Type", "Source URL", "Processed At"])
    
    # 🧼 ULTRA-SAFE DATA CLEANING: Force convert everything to string elements & strip spaces
    df["Type"] = df["Type"].fillna("HN-STORY").astype(str)
    df["Sentiment"] = df["Sentiment"].fillna("Neutral").astype(str)
    
    # Calculate high level stream insights
    total_messages = len(df)
    
    # Standardize counts dynamically
    sentiment_counts = df["Sentiment"].value_counts().to_dict()
    pos_count = sentiment_counts.get("Positive", 0) + sentiment_counts.get("positive", 0)
    neg_count = sentiment_counts.get("Negative", 0) + sentiment_counts.get("negative", 0)
    neu_count = sentiment_counts.get("Neutral", 0) + sentiment_counts.get("neutral", 0)
    
    # High-level Metrics Grid
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Total Items Captured", value=total_messages)
    with col2:
        st.metric(label="🟢 Positive Posts", value=pos_count)
    with col3:
        st.metric(label="🔴 Negative Posts", value=neg_count)
    with col4:
        st.metric(label="⚪ Neutral Discussions", value=neu_count)
        
    st.markdown("---")
    
    # Dynamic Visualizations Panel
    col_chart, col_empty = st.columns([2, 1])
    with col_chart:
        st.subheader("📊 Sentiment Market Share")
        # Map structured sentiment counts to simple bars
        chart_df = pd.DataFrame({
            'Sentiment': ['Positive', 'Neutral', 'Negative'],
            'Count': [pos_count, neu_count, neg_count]
        }).set_index('Sentiment')
        st.bar_chart(chart_df, height=220)
        
    st.markdown("---")
    
    # Render Interactive Table Layout
    st.subheader("📋 Latest Live Hacker News Insights")
    
    # Secure row formatting with inline string casting to prevent any future float checks crashing the parser
    df["Feed Type"] = df["Type"].apply(lambda x: f"🗣️ {x}" if "COMMENT" in str(x).upper() else f"📰 {x}")
    
    st.dataframe(
        df[["Feed Type", "AI Semantic Summary", "Sentiment", "Raw Text", "Processed At"]],
        column_config={
            "Feed Type": st.column_config.TextColumn(width="small"),
            "AI Semantic Summary": st.column_config.TextColumn(width="large"),
            "Sentiment": st.column_config.TextColumn(width="small"),
            "Raw Text": st.column_config.TextColumn(width="medium"),
            "Processed At": st.column_config.DatetimeColumn(format="HH:mm:ss")
        },
        use_container_width=True,
        hide_index=True
    )

# Client-side auto-refresh cycle controller
st.caption("🔄 Dashboard auto-refresh cycle active.")
asyncio.run(asyncio.sleep(refresh_rate))
st.rerun()