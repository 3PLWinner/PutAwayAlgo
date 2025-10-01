import os
import pandas as pd
import streamlit as st
from difflib import SequenceMatcher

CSV_FOLDER = "csvs"
LOCATIONS_FILE = "locations.csv"
UNITS_FILE = "units.csv"


def load_reports():
    locs = pd.read_csv(os.path.join(CSV_FOLDER, LOCATIONS_FILE))
    units = pd.read_csv(os.path.join(CSV_FOLDER, UNITS_FILE))
    
    # Create LocationKey
    for df in [locs, units]:
        df["LocationKey"] = (
            df["Aisle"].astype(str).str.strip() + "-" +
            df["Rack"].astype(str).str.strip() + "-" +
            df["Level"].astype(str).str.strip()
        )
    
    # Summarize units by location
    units_summary = units.groupby("LocationKey", as_index=False).agg({
        "Total On Hand": "sum",
        "Product ID": "first"  # Keep product info for similarity matching
    })
    units_summary.rename(columns={"Total On Hand": "OnHandQty"}, inplace=True)
    
    return locs, units, units_summary


def get_available_locations(locs, units_summary):
    merged = locs.merge(units_summary, on="LocationKey", how="left")
    merged["OnHandQty"] = merged["OnHandQty"].fillna(0)
    merged["Product ID"] = merged["Product ID"].fillna("")
    
    # Only B or F levels
    merged = merged[merged["Level"].str.upper().str.contains("B|F", na=False)]
    
    # Available if OPEN or INUSE with 0 on-hand
    available = merged[
        (merged["Location Status"].str.upper() == "OPEN") |
        ((merged["Location Status"].str.upper() == "INUSE") & (merged["OnHandQty"] == 0))
    ]
    return available


def calculate_similarity(product1, product2):
    """Calculate similarity between two product IDs"""
    if pd.isna(product1) or pd.isna(product2) or product1 == "" or product2 == "":
        return 0
    return SequenceMatcher(None, str(product1).upper(), str(product2).upper()).ratio()


def get_aisle_rack_locations(available, aisle, rack):
    """Get all locations in the same aisle and rack"""
    return available[
        (available["Aisle"] == aisle) & 
        (available["Rack"] == rack)
    ]


def recommend_location(product_id, units, available):
    """
    Recommend location based on:
    1. PRIORITY 1: Same aisle-rack where product already exists
    2. PRIORITY 2: Front if back is filled, back if front is empty
    3. PRIORITY 3: Close to similar products
    """
    if available.empty:
        return None, "No available locations"
    
    # Get all locations where this exact product currently exists
    existing_product_units = units[units["Product ID"] == product_id]
    
    if existing_product_units.empty:
        # Product doesn't exist anywhere - find similar products
        all_products = units["Product ID"].unique()
        similar_products = []
        for prod in all_products:
            if prod != product_id and calculate_similarity(product_id, prod) > 0.6:
                similar_products.append(prod)
        
        similar_product_locations = units[units["Product ID"].isin(similar_products)]["LocationKey"].unique()
        return recommend_new_product_location(available, similar_products, similar_product_locations)
    
    # Product exists - find best location in same aisle-rack first
    existing_aisles_racks = existing_product_units[["Aisle", "Rack"]].drop_duplicates()
    
    print(f"🔍 Product {product_id} exists in {len(existing_aisles_racks)} aisle-rack combinations:")
    for _, ar in existing_aisles_racks.iterrows():
        print(f"  - Aisle {ar['Aisle']}, Rack {ar['Rack']}")
    
    recommendations = []
    
    # PRIORITY 1: Check aisle-racks where product already exists
    for _, row in existing_aisles_racks.iterrows():
        aisle, rack = row["Aisle"], row["Rack"]
        locations_in_section = get_aisle_rack_locations(available, aisle, rack)
        
        if locations_in_section.empty:
            print(f"  ❌ No available locations in aisle {aisle}, rack {rack}")
            continue
        
        print(f"  ✅ Found {len(locations_in_section)} available locations in aisle {aisle}, rack {rack}")
        
        # Get the occupancy pattern for this aisle-rack (including occupied locations)
        all_locations_in_section = units[
            (units["Aisle"] == aisle) & (units["Rack"] == rack)
        ]
        
        # Separate front and back for occupancy analysis
        front_occupied = len(all_locations_in_section[
            all_locations_in_section["Level"].str.upper().str.contains("F") & 
            (all_locations_in_section["Total On Hand"] > 0)
        ])
        back_occupied = len(all_locations_in_section[
            all_locations_in_section["Level"].str.upper().str.contains("B") & 
            (all_locations_in_section["Total On Hand"] > 0)
        ])
        
        # Available locations in this section
        front_locs = locations_in_section[locations_in_section["Level"].str.upper().str.contains("F")]
        back_locs = locations_in_section[locations_in_section["Level"].str.upper().str.contains("B")]
        
        front_available = len(front_locs)
        back_available = len(back_locs)
        
        print(f"    Front: {front_occupied} occupied, {front_available} available")
        print(f"    Back: {back_occupied} occupied, {back_available} available")
        
        # Determine preferred level based on occupancy
        preferred_locations = pd.DataFrame()
        logic_used = ""
        
        # If back is filled and front has space, prefer front
        if back_occupied > 0 and front_available > 0:
            preferred_locations = front_locs
            logic_used = "Front (back occupied)"
        # If front is empty and back has space, prefer back
        elif front_occupied == 0 and back_available > 0:
            preferred_locations = back_locs
            logic_used = "Back (front empty)"
        # Otherwise, prefer front if available
        elif front_available > 0:
            preferred_locations = front_locs
            logic_used = "Front (default)"
        # Fall back to back if front not available
        elif back_available > 0:
            preferred_locations = back_locs
            logic_used = "Back (front full)"
        
        # Score locations in this section
        for _, loc in preferred_locations.iterrows():
            score = 1000  # High base score for same aisle-rack as existing product
            proximity_reason = [f"Same product in aisle {aisle}, rack {rack}"]
            
            recommendations.append({
                "location": loc,
                "score": score,
                "logic": logic_used,
                "proximity": ", ".join(proximity_reason),
                "aisle_rack": f"{aisle}-{rack}"
            })
    
    # If no locations found in existing aisle-racks, fall back to similar products
    if not recommendations:
        print("⚠️ No available locations in existing aisle-racks, checking similar products...")
        all_products = units["Product ID"].unique()
        similar_products = []
        for prod in all_products:
            if prod != product_id and calculate_similarity(product_id, prod) > 0.6:
                similar_products.append(prod)
        
        similar_product_locations = units[units["Product ID"].isin(similar_products)]["LocationKey"].unique()
        return recommend_new_product_location(available, similar_products, similar_product_locations)
    
    # Sort by score (highest first)
    recommendations.sort(key=lambda x: x["score"], reverse=True)
    best = recommendations[0]
    
    return best["location"], f"Logic: {best['logic']}, Proximity: {best['proximity']}"


def recommend_new_product_location(available, similar_products, similar_product_locations):
    """Handle recommendation for products that don't exist yet"""
    recommendations = []
    
    # Check each aisle-rack combination
    aisle_racks = available[["Aisle", "Rack"]].drop_duplicates()
    
    for _, row in aisle_racks.iterrows():
        aisle, rack = row["Aisle"], row["Rack"]
        locations_in_section = get_aisle_rack_locations(available, aisle, rack)
        
        if locations_in_section.empty:
            continue
        
        # Check if this aisle-rack has similar products
        section_score_bonus = 0
        proximity_reason = []
        
        for _, loc in locations_in_section.iterrows():
            if loc["LocationKey"] in similar_product_locations:
                section_score_bonus = 50
                proximity_reason.append("Similar products nearby")
                break
        
        # Separate front and back locations
        front_locs = locations_in_section[locations_in_section["Level"].str.upper().str.contains("F")]
        back_locs = locations_in_section[locations_in_section["Level"].str.upper().str.contains("B")]
        
        # Check occupancy of front and back
        front_occupied = len(front_locs[front_locs["OnHandQty"] > 0])
        back_occupied = len(back_locs[back_locs["OnHandQty"] > 0])
        
        front_available = len(front_locs[front_locs["OnHandQty"] == 0])
        back_available = len(back_locs[back_locs["OnHandQty"] == 0])
        
        # Determine preferred level based on occupancy
        preferred_locations = pd.DataFrame()
        logic_used = ""
        
        # If back is filled and front has space, prefer front
        if back_occupied > 0 and front_available > 0:
            preferred_locations = front_locs[front_locs["OnHandQty"] == 0]
            logic_used = "Front (back occupied)"
        # If front is empty and back has space, prefer back
        elif front_occupied == 0 and back_available > 0:
            preferred_locations = back_locs[back_locs["OnHandQty"] == 0]
            logic_used = "Back (front empty)"
        # Otherwise, prefer front if available
        elif front_available > 0:
            preferred_locations = front_locs[front_locs["OnHandQty"] == 0]
            logic_used = "Front (default)"
        # Fall back to back if front not available
        elif back_available > 0:
            preferred_locations = back_locs[back_locs["OnHandQty"] == 0]
            logic_used = "Back (front full)"
        
        # Score locations in this section
        for _, loc in preferred_locations.iterrows():
            score = section_score_bonus
            
            recommendations.append({
                "location": loc,
                "score": score,
                "logic": logic_used,
                "proximity": ", ".join(proximity_reason) if proximity_reason else "No similar products nearby",
                "aisle_rack": f"{aisle}-{rack}"
            })
    
    if not recommendations:
        return None, "No suitable locations found"
    
    # Sort by score (highest first)
    recommendations.sort(key=lambda x: x["score"], reverse=True)
    best = recommendations[0]
    
    return best["location"], f"Logic: {best['logic']}, Proximity: {best['proximity']}"


# Streamlit UI
st.title("🏭 Product Put-Away Recommender")
st.markdown("*Intelligent location assignment for warehouse receiving*")

# Load data
try:
    locs, units, units_summary = load_reports()
    available = get_available_locations(locs, units_summary)
    
    st.sidebar.success(f"✅ Data loaded successfully")
    st.sidebar.info(f"📊 **Available Locations:** {len(available)}")
    st.sidebar.info(f"📦 **Total Products:** {len(units['Product ID'].unique())}")
    
except FileNotFoundError as e:
    st.error(f"❌ Error loading data: {e}")
    st.info("Make sure to run PutAway.py first to generate the required CSV files.")
    st.stop()

# Main interface
col1, col2 = st.columns([2, 1])

with col1:
    product_id = st.text_input("📦 Enter Product ID:", placeholder="e.g., ABC123")

with col2:
    st.write("")  # Spacing
    recommend_button = st.button("🎯 Recommend Location", type="primary")

if recommend_button and product_id:
    with st.spinner("🔍 Finding optimal location..."):
        recommendation, reasoning = recommend_location(product_id, units, available)
    
    if recommendation is None:
        st.warning(f"⚠️ {reasoning}")
    else:
        st.success(f"✅ **Recommended Location:** `{recommendation['LocationKey']}`")
        
        # Display details
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📍 Level", recommendation['Level'])
        with col2:
            st.metric("📊 Status", recommendation['Location Status'])
        with col3:
            st.metric("📦 On Hand", int(recommendation['OnHandQty']))
        
        st.info(f"**Reasoning:** {reasoning}")
        
        # Show existing locations for this product
        existing_locs = units[units["Product ID"] == product_id]
        if not existing_locs.empty:
            st.subheader("📍 Current Locations for This Product")
            display_existing = existing_locs[["LocationKey", "Total On Hand"]].groupby("LocationKey").sum().reset_index()
            st.dataframe(display_existing, use_container_width=True)
        
        # Add button to actually move a unit to this location
        if st.button("🚚 Move Unit to This Location", type="secondary"):
            try:
                from putaway_integration import VeraCoreAPI
                
                # You'll need to get the Unit ID for the specific unit to move
                unit_id = st.text_input("Enter Unit ID to move:", key="unit_move")
                
                if unit_id:
                    api = VeraCoreAPI()
                    
                    # Get Location ID from the recommendation
                    location_id = api.get_location_id_from_key(recommendation['LocationKey'], locs)
                    
                    if location_id:
                        with st.spinner("Moving unit..."):
                            success = api.move_unit_to_location(unit_id, location_id)
                        
                        if success:
                            st.success(f"✅ Unit {unit_id} successfully moved to {recommendation['LocationKey']}")
                            st.rerun()  # Refresh data
                        else:
                            st.error("❌ Failed to move unit. Check logs for details.")
                    else:
                        st.error("❌ Could not find Location ID for this location.")
                        
            except ImportError:
                st.info("💡 Install the integration module to enable unit movement via API")

# Show all available locations
with st.expander("📋 View All Available Locations"):
    if not available.empty:
        display_df = available[["LocationKey", "Aisle", "Rack", "Level", "Location Status", "OnHandQty", "Product ID"]].copy()
        display_df["Product ID"] = display_df["Product ID"].fillna("Empty")
        st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No available locations found.")

# Analytics section
if st.checkbox("📈 Show Analytics"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Location Status Distribution")
        status_counts = available["Location Status"].value_counts()
        st.bar_chart(status_counts)
    
    with col2:
        st.subheader("Available Locations by Level")
        level_counts = available["Level"].value_counts()
        st.bar_chart(level_counts)