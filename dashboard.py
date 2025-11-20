import dashboard as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path

# Page configuration
st.set_page_config(page_title="Obie Premium Reconciliation", layout="wide")

# Data Loading

@st.cache_data
def load_data():
    """Load all CSV files with error handling."""
    try:
        dq_issues = pd.read_csv("output/data_quality_issues.csv")
    except FileNotFoundError:
        dq_issues = pd.DataFrame(columns=["table_name", "check_name", "policy_id", "issue_detail"])
    
    try:
        reconciliation = pd.read_csv("output/reconciliation_results.csv")
    except FileNotFoundError:
        reconciliation = pd.DataFrame(columns=["policy_id", "premium_policy", "premium_gl", "diff", "diff_pct", "flag_reason"])
    
    try:
        reporting = pd.read_csv("output/reporting_dataset.csv")
        # Convert booking_date to datetime for proper sorting
        if "booking_date" in reporting.columns:
            reporting["booking_date"] = pd.to_datetime(reporting["booking_date"])
    except FileNotFoundError:
        reporting = pd.DataFrame(columns=["booking_date", "state", "total_policy_premium", "total_gl_premium", "variance"])
    
    try:
        claims = pd.read_csv("data/claims.csv")
    except FileNotFoundError:
        claims = pd.DataFrame(columns=["state", "incurred_loss", "paid_loss"])
    
    return dq_issues, reconciliation, reporting, claims

def regenerate_data():
    """Regenerate data by calling backend scripts."""
    try:
        st.info("🔄 Retrieving New data...")
        import generate_data
        generate_data.main()
        
        st.info("🔍 Running data quality checks and reconciliation...")
        import dq_and_reconcile
        dq_and_reconcile.main()
        
        st.success("✅ Data Retrieved successfully!")
        st.cache_data.clear()
        st.rerun()
    except Exception as e:
        st.error(f"❌ Error regenerating data: {str(e)}")


# Load data
dq_issues, reconciliation, reporting, claims = load_data()

# Header and regenerate button
col_title, col_button = st.columns([4, 1])
with col_title:
    st.title("🏢 Obie Premium Reconciliation Monitor")
    st.caption("Real-time premium reconciliation, data quality monitoring, and loss ratio analytics")
with col_button:
    st.write("")  # spacing
    if st.button("🔄 Regenerate Data", use_container_width=True):
        regenerate_data()

st.divider()

# TOP METRICS ROW

total_policy_premium = reconciliation["premium_policy"].sum() if len(reconciliation) > 0 else 0
total_gl_premium = reconciliation["premium_gl"].sum() if len(reconciliation) > 0 else 0
delta_premium = total_gl_premium - total_policy_premium
num_dq_issues = len(dq_issues)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="📊 Total Policy Premium",
        value=f"${total_policy_premium:,.2f}"
    )

with col2:
    st.metric(
        label="📈 Total GL Premium",
        value=f"${total_gl_premium:,.2f}",
        delta=f"${delta_premium:,.2f} vs Policy"
    )

with col3:
    st.metric(
        label="⚠️ Data Quality Issues",
        value=f"{num_dq_issues:,}"
    )

st.divider()

# SIDEBAR 
st.sidebar.header("ℹ️ About")

st.sidebar.info("""
**Premium Reconciliation Monitor**

This dashboard helps you:

• **Monitor Data Quality** - Identify and track data issues across policy and GL systems

• **Reconcile Premiums** - Compare policy premiums vs general ledger premiums to catch discrepancies

• **Analyze Trends** - Visualize premium trends over time and across states

• **Track Loss Ratios** - Monitor claims performance and loss ratios by state

Use the filters within each tab to drill down into specific segments of your data.
""")

# TABS
# Custom CSS to make tabs equal width
st.markdown("""
    <style>
        .stTabs [data-baseweb="tab-list"] {
            gap: 0px;
            width: 100%;
        }
        .stTabs [data-baseweb="tab"] {
            flex: 1;
            white-space: pre-wrap;
            justify-content: center;
        }
    </style>
""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs(["📋 Data Quality", "🔄 Reconciliation", "📈 Reporting Trends", "💰 Claims & Loss Ratio"])


# TAB 1: DATA QUALITY

with tab1:
    st.subheader("Data Quality Issues")
    st.caption("Records failing data quality rules")
    
    # Tab-specific filters
    col_f1, col_f2, col_f3 = st.columns(3)
    
    with col_f1:
        dq_tables = ["All tables"] + sorted(dq_issues["table_name"].dropna().unique().tolist()) if len(dq_issues) > 0 else ["All tables"]
        selected_dq_table = st.selectbox(
            "Filter by Table",
            options=dq_tables,
            key="dq_table_filter"
        )
    
    with col_f2:
        dq_checks = ["All checks"] + sorted(dq_issues["check_name"].dropna().unique().tolist()) if len(dq_issues) > 0 else ["All checks"]
        selected_dq_check = st.selectbox(
            "Filter by Check",
            options=dq_checks,
            key="dq_check_filter"
        )
    
    with col_f3:
        show_all_rows = st.checkbox("Show all rows", value=False, key="show_all_dq")
    
    # Apply filters
    filtered_dq = dq_issues.copy()
    
    if selected_dq_table != "All tables":
        filtered_dq = filtered_dq[filtered_dq["table_name"] == selected_dq_table]
    
    if selected_dq_check != "All checks":
        filtered_dq = filtered_dq[filtered_dq["check_name"] == selected_dq_check]
    
    st.divider()
    
    # DQ Summary
    if len(filtered_dq) > 0:
        st.write("**📊 Data Quality Summary**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Issues by check name
            check_summary = filtered_dq["check_name"].value_counts().reset_index()
            check_summary.columns = ["Check Name", "Count"]
            st.dataframe(check_summary, use_container_width=True, height=200)
        
        with col2:
            # Issues by table
            table_summary = filtered_dq["table_name"].value_counts().reset_index()
            table_summary.columns = ["Table Name", "Count"]
            st.dataframe(table_summary, use_container_width=True, height=200)
        
        st.divider()
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Issues (Filtered)", len(filtered_dq))
    with col2:
        rows_to_show = len(filtered_dq) if show_all_rows else min(200, len(filtered_dq))
        st.metric("Displaying", rows_to_show)
    with col3:
        # Download button
        if len(filtered_dq) > 0:
            st.download_button(
                "📥 Download DQ Issues",
                filtered_dq.to_csv(index=False).encode("utf-8"),
                file_name="data_quality_issues_filtered.csv",
                mime="text/csv",
                use_container_width=True
            )
    
    # Display dataframe
    if len(filtered_dq) > 0:
        display_cols = ["table_name", "check_name", "policy_id", "issue_detail"]
        available_cols = [col for col in display_cols if col in filtered_dq.columns]
        
        rows_to_display = filtered_dq[available_cols] if show_all_rows else filtered_dq[available_cols].head(200)
        
        st.dataframe(
            rows_to_display,
            use_container_width=True,
            height=400
        )
    else:
        st.success("✅ No data quality issues found!")


# TAB 2: RECONCILIATION

with tab2:
    st.subheader("Premium Reconciliation Results")
    st.caption("Top discrepancies between Policy and GL systems")
    
    # Tab-specific filters
    col_f1, col_f2 = st.columns(2)
    
    with col_f1:
        recon_flag_options = ["All", "OK", "Large difference", "Missing in policies", "Missing in GL"]
        selected_recon_flag = st.selectbox(
            "Filter by Reconciliation Status",
            options=recon_flag_options,
            key="recon_flag_filter"
        )
    
    with col_f2:
        top_n = st.number_input("Show top N policies", min_value=10, max_value=500, value=20, step=10, key="top_n_recon")
    
    # Apply reconciliation flag filter
    filtered_recon = reconciliation.copy()
    
    if selected_recon_flag != "All":
        filtered_recon = filtered_recon[filtered_recon["flag_reason"] == selected_recon_flag]
    
    # Add absolute difference column
    if len(filtered_recon) > 0:
        filtered_recon["abs_diff"] = filtered_recon["diff"].abs()
        
        # Sort by absolute difference and get top N
        top_discrepancies = filtered_recon.nlargest(top_n, "abs_diff")
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Policies (Filtered)", len(filtered_recon))
        with col2:
            avg_diff = filtered_recon["diff"].mean()
            st.metric("Avg Difference", f"${avg_diff:,.2f}")
        with col3:
            max_diff = filtered_recon["abs_diff"].max()
            st.metric("Max Difference", f"${max_diff:,.2f}")
        with col4:
            # Download button
            st.download_button(
                "📥 Download",
                top_discrepancies.to_csv(index=False).encode("utf-8"),
                file_name="reconciliation_filtered.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        st.divider()
        st.write(f"**Top {top_n} Policies by Absolute Difference**")
        
        # Format and style the dataframe
        display_recon = top_discrepancies[["policy_id", "premium_policy", "premium_gl", "diff", "diff_pct", "flag_reason"]].copy()
        
        def color_diff(val):
            if pd.isna(val):
                return ""
            color = "green" if val > 0 else "red" if val < 0 else "gray"
            return f"color: {color}"
        
        styled_df = display_recon.style.applymap(color_diff, subset=["diff", "diff_pct"])
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.info("No reconciliation data available.")

# TAB 3: REPORTING TRENDS

with tab3:
    st.subheader("Premium Trends Analysis")
    st.caption("Policy vs GL premium trends over time and by state")
    
    # Tab-specific filters
    col_f1, col_f2 = st.columns([3, 1])
    
    with col_f1:
        available_states = sorted(reporting["state"].dropna().unique()) if len(reporting) > 0 else []
        selected_states = st.multiselect(
            "Filter by State(s)",
            options=available_states,
            default=available_states,
            key="reporting_state_filter"
        )
    
    with col_f2:
        st.write("")  
        st.write("")  
        
    # Apply state filter
    filtered_reporting = reporting.copy()
    if selected_states:
        filtered_reporting = filtered_reporting[filtered_reporting["state"].isin(selected_states)]
    
    if len(filtered_reporting) > 0:
        # Download button in top right
        col_metrics, col_download = st.columns([3, 1])
        with col_download:
            st.download_button(
                "📥 Download Trends",
                filtered_reporting.to_csv(index=False).encode("utf-8"),
                file_name="reporting_trends_filtered.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        st.divider()
        
        # Time series chart
        st.write("**Premium Trends Over Time**")
        
        # Aggregate by booking date
        if "booking_date" in filtered_reporting.columns:
            time_series = filtered_reporting.groupby("booking_date").agg({
                "total_policy_premium": "sum",
                "total_gl_premium": "sum"
            }).reset_index()
            
            # Reshape for line chart
            time_series_melted = time_series.melt(
                id_vars="booking_date",
                value_vars=["total_policy_premium", "total_gl_premium"],
                var_name="Premium Type",
                value_name="Amount"
            )
            
            st.line_chart(
                time_series_melted,
                x="booking_date",
                y="Amount",
                color="Premium Type",
                height=300
            )
        
        st.write("**Premium by State**")
        
        # Aggregate by state
        state_summary = filtered_reporting.groupby("state").agg({
            "total_policy_premium": "sum",
            "total_gl_premium": "sum"
        }).reset_index()
        
        # Create grouped bar chart using plotly-style data
        import plotly.graph_objects as go
        
        fig = go.Figure(data=[
            go.Bar(name='Policy Premium', x=state_summary['state'], y=state_summary['total_policy_premium']),
            go.Bar(name='GL Premium', x=state_summary['state'], y=state_summary['total_gl_premium'])
        ])
        
        fig.update_layout(
            barmode='group',
            height=350,
            xaxis_title="State",
            yaxis_title="Premium Amount ($)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.write("**Detailed Reporting Data**")
        
        # Add variance coloring
        def color_variance(val):
            if pd.isna(val):
                return ""
            color = "green" if val >= 0 else "red"
            return f"color: {color}"
        
        display_reporting = filtered_reporting[["booking_date", "state", "total_policy_premium", "total_gl_premium", "variance"]].copy()
        styled_reporting = display_reporting.style.applymap(color_variance, subset=["variance"])
        st.dataframe(styled_reporting, use_container_width=True, height=300)
    else:
        st.info("No reporting data available for selected filters.")


# TAB 4: CLAIMS & LOSS RATIO

with tab4:
    st.subheader("Claims Analysis & Loss Ratios")
    st.caption("Incurred and paid losses with loss ratio calculations")
    
    # Tab-specific filters
    col_f1, col_f2 = st.columns([3, 1])
    
    with col_f1:
        available_states_claims = sorted(reporting["state"].dropna().unique()) if len(reporting) > 0 else []
        selected_states_claims = st.multiselect(
            "Filter by State(s)",
            options=available_states_claims,
            default=available_states_claims,
            key="claims_state_filter"
        )
    
    if len(claims) > 0 and total_policy_premium > 0:
        # Calculate totals
        total_incurred = claims["incurred_loss"].sum()
        total_paid = claims["paid_loss"].sum()
        overall_loss_ratio = (total_incurred / total_policy_premium) * 100
        
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("💵 Total Incurred Loss", f"${total_incurred:,.2f}")
        with col2:
            st.metric("💳 Total Paid Loss", f"${total_paid:,.2f}")
        with col3:
            st.metric("📊 Overall Loss Ratio", f"{overall_loss_ratio:.2f}%")
        
        st.divider()
        
        # Group by state
        if "state" in claims.columns:
            # Get premium by state from reporting data
            state_premiums = reporting.groupby("state")["total_policy_premium"].sum().reset_index()
            
            # Group claims by state
            state_claims = claims.groupby("state").agg({
                "incurred_loss": "sum",
                "paid_loss": "sum"
            }).reset_index()
            
            # Merge with premiums
            state_analysis = state_claims.merge(state_premiums, on="state", how="left")
            state_analysis["loss_ratio"] = (state_analysis["incurred_loss"] / state_analysis["total_policy_premium"]) * 100
            
            # Apply state filter
            if selected_states_claims:
                state_analysis = state_analysis[state_analysis["state"].isin(selected_states_claims)]
            
            # Download button
            col_title, col_download = st.columns([3, 1])
            with col_download:
                st.download_button(
                    "📥 Download Claims",
                    state_analysis.to_csv(index=False).encode("utf-8"),
                    file_name="claims_analysis_filtered.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            # Rename columns for display
            state_analysis_display = state_analysis.copy()
            state_analysis_display.columns = ["state", "total_incurred_loss", "total_paid_loss", "total_policy_premium", "loss_ratio"]
            
            st.write("**Loss Ratio by State**")
            
            # Bar chart of loss ratios
            st.bar_chart(
                state_analysis_display,
                x="state",
                y="loss_ratio",
                height=300
            )
            
            st.write("**Detailed State Analysis**")
            
            # Format the display
            display_state = state_analysis_display.copy()
            display_state["total_policy_premium"] = display_state["total_policy_premium"].apply(lambda x: f"${x:,.2f}")
            display_state["total_incurred_loss"] = display_state["total_incurred_loss"].apply(lambda x: f"${x:,.2f}")
            display_state["total_paid_loss"] = display_state["total_paid_loss"].apply(lambda x: f"${x:,.2f}")
            display_state["loss_ratio"] = display_state["loss_ratio"].apply(lambda x: f"{x:.2f}%")
            
            st.dataframe(display_state, use_container_width=True, height=300)
        else:
            st.dataframe(claims.head(100), use_container_width=True)
    else:
        st.info("No claims data available or premium data is missing.")

# Footer

st.divider()
st.caption("💡 Tip: Use the sidebar filters to drill down into specific states, reconciliation statuses, or data quality issues.")