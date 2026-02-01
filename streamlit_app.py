"""
Seven Gravity Automation Hub
Streamlit web interface for lead qualification and processing tools.

Run locally: streamlit run execution/streamlit_app.py
"""

import os
import sys
import json
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st
import pandas as pd

# Add execution directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Page config
st.set_page_config(
    page_title="Seven Gravity Automation Hub",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Password protection
def check_password():
    """Simple password protection"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    # Check for password in environment or use default
    correct_password = os.getenv("STREAMLIT_PASSWORD", "sevengravity2026")

    st.title("🔐 Seven Gravity Automation Hub")
    st.markdown("Enter password to access the tools.")

    password = st.text_input("Password", type="password", key="password_input")

    if st.button("Login", type="primary"):
        if password == correct_password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")

    return False


# Import tools (after path setup)
def import_tools():
    """Lazy import of tools to avoid startup errors"""
    tools = {}

    try:
        from lead_ingest import ingest_file, save_normalized
        tools['lead_ingest'] = {'ingest_file': ingest_file, 'save_normalized': save_normalized}
    except ImportError as e:
        tools['lead_ingest'] = None

    try:
        from deduplicate_companies import deduplicate_companies
        tools['deduplicate_companies'] = deduplicate_companies
    except ImportError:
        tools['deduplicate_companies'] = None

    try:
        from deduplicate_contacts import deduplicate_contacts
        tools['deduplicate_contacts'] = deduplicate_contacts
    except ImportError:
        tools['deduplicate_contacts'] = None

    try:
        from company_type_filter import filter_companies
        tools['company_type_filter'] = filter_companies
    except ImportError:
        tools['company_type_filter'] = None

    try:
        from icp_scorer import score_leads
        tools['icp_scorer'] = score_leads
    except ImportError:
        tools['icp_scorer'] = None

    try:
        from calculate_lead_score import score_all_leads
        tools['calculate_lead_score'] = score_all_leads
    except ImportError:
        tools['calculate_lead_score'] = None

    try:
        from output_tam import output_tam
        tools['output_tam'] = output_tam
    except ImportError:
        tools['output_tam'] = None

    try:
        from identify_decision_makers import process_excel_file
        tools['identify_decision_makers'] = process_excel_file
    except ImportError:
        tools['identify_decision_makers'] = None

    try:
        from normalize_company_name import normalize_batch, normalize_company_name
        tools['normalize_company_name'] = normalize_batch
        tools['normalize_single'] = normalize_company_name
    except ImportError:
        tools['normalize_company_name'] = None

    try:
        from categorize_company_niche import categorize_niche
        tools['categorize_niche'] = categorize_niche
    except ImportError:
        tools['categorize_niche'] = None

    try:
        from score_industries import score_industries_batch, extract_industries_from_csv
        tools['score_industries'] = score_industries_batch
        tools['extract_industries'] = extract_industries_from_csv
    except ImportError:
        tools['score_industries'] = None

    try:
        from blitz_api import BlitzAPI
        tools['blitz_api'] = BlitzAPI
    except ImportError:
        tools['blitz_api'] = None

    try:
        from millionverifier_api import verify_emails as mv_verify
        tools['millionverifier'] = mv_verify
    except ImportError:
        tools['millionverifier'] = None

    try:
        from bounceban_api import verify_emails as bb_verify
        tools['bounceban'] = bb_verify
    except ImportError:
        tools['bounceban'] = None

    try:
        from triple_verify_emails import triple_verify_leads
        tools['triple_verify'] = triple_verify_leads
    except ImportError:
        tools['triple_verify'] = None

    return tools


def get_config_files():
    """Get list of available campaign config files"""
    config_dir = Path(__file__).parent / "configs"
    if config_dir.exists():
        # Exclude schema and reference files
        exclude = ["campaign_config_schema.json", "target_roles_default.json"]
        configs = [f.name for f in config_dir.glob("*.json")
                   if not f.name.startswith("_") and f.name not in exclude]
        # Put general.json first if it exists
        if "general.json" in configs:
            configs.remove("general.json")
            configs.insert(0, "general.json")
        return configs
    return []


def load_config(config_name):
    """Load a config file"""
    config_path = Path(__file__).parent / "configs" / config_name
    if config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    return {}


def save_uploaded_file(uploaded_file):
    """Save uploaded file to temp directory and return path"""
    temp_dir = Path(__file__).parent.parent / ".tmp" / "uploads"
    temp_dir.mkdir(parents=True, exist_ok=True)

    file_path = temp_dir / uploaded_file.name
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    return str(file_path)


# ============== PAGES ==============

def page_home():
    """Home page with overview"""
    st.title("🚀 Seven Gravity Automation Hub")
    st.markdown("Welcome! Select a tool from the sidebar to get started.")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📋 Lead Tools")
        st.markdown("""
        - **Lead Qualification Pipeline** - Full end-to-end processing
        - **Identify Decision Makers** - Classify contacts by title
        - **Score Industries** - Evaluate industry viability
        - **Categorize Niche** - Determine company niche
        - **Normalize Company Names** - Clean names for emails
        - **Deduplicate** - Remove duplicate companies/contacts
        """)

        st.markdown("### 🔍 Single Lookups")
        st.markdown("""
        - **Find Decision Makers** - Look up DMs at one company
        - **Get Work Email** - Find email for one person
        - **Company Info** - Get firmographics for one company
        """)

    with col2:
        st.markdown("### ✉️ Email Tools")
        st.markdown("""
        - **Triple Verify Emails** - BlitzAPI → MV → BB
        - **Million Verifier** - First-pass verification
        - **Bounce Ban** - Catch-all verification
        """)

        st.markdown("### ⚙️ Configuration")
        st.markdown("""
        - Configs stored in `execution/configs/`
        - Create new configs for different campaigns
        - Each config defines ICP criteria, filters, output format
        """)


def page_lead_pipeline():
    """Full lead qualification pipeline"""
    st.title("📋 Lead Qualification Pipeline")
    st.markdown("Upload a file and run the complete qualification workflow.")

    tools = import_tools()

    # File upload
    uploaded_file = st.file_uploader(
        "Upload Excel or CSV file",
        type=["xlsx", "xls", "csv"],
        help="File with leads to qualify"
    )

    # Config selection
    configs = get_config_files()
    selected_config = st.selectbox(
        "Select Campaign Config",
        configs if configs else ["No configs found"],
        help="Configuration file with ICP criteria and filters"
    )

    # Pipeline options
    st.markdown("### Pipeline Options")
    col1, col2 = st.columns(2)

    with col1:
        run_dedup = st.checkbox("Run Deduplication", value=True)
        run_filter = st.checkbox("Run Company Filter", value=True)
        run_icp = st.checkbox("Run ICP Scoring", value=True)

    with col2:
        run_dm = st.checkbox("Identify Decision Makers", value=True)
        run_normalize = st.checkbox("Normalize Company Names", value=True)
        run_verify = st.checkbox("Triple Verify Emails", value=False,
                                 help="Uses API credits - BlitzAPI, MV, BB")

    # Run button
    if st.button("🚀 Run Pipeline", type="primary", disabled=not uploaded_file):
        if not uploaded_file:
            st.error("Please upload a file first")
            return

        config = load_config(selected_config) if selected_config != "No configs found" else {}

        # Create temp directories
        tmp_dir = Path(__file__).parent.parent / ".tmp"
        leads_dir = tmp_dir / "leads"
        output_dir = tmp_dir / "output"
        leads_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save uploaded file
        input_path = save_uploaded_file(uploaded_file)

        progress = st.progress(0)
        status = st.empty()

        try:
            # Step 1: Ingest
            status.text("Step 1/7: Ingesting leads...")
            progress.progress(10)

            if tools.get('lead_ingest'):
                leads = tools['lead_ingest']['ingest_file'](input_path)
                normalized_path = str(leads_dir / "normalized.json")
                tools['lead_ingest']['save_normalized'](leads, normalized_path)
                current_path = normalized_path
                st.success(f"✓ Ingested {len(leads)} leads")
            else:
                st.warning("Lead ingest tool not available")
                return

            # Step 2: Deduplicate Companies
            if run_dedup:
                status.text("Step 2/7: Deduplicating companies...")
                progress.progress(25)

                if tools.get('deduplicate_companies'):
                    dedup_path = str(leads_dir / "deduped.json")
                    tools['deduplicate_companies'](current_path, dedup_path)
                    current_path = dedup_path
                    st.success("✓ Company deduplication complete")

                # Deduplicate contacts
                if tools.get('deduplicate_contacts'):
                    contacts_path = str(leads_dir / "contacts_deduped.json")
                    tools['deduplicate_contacts'](current_path, contacts_path)
                    current_path = contacts_path
                    st.success("✓ Contact deduplication complete")

            # Step 3: Company Filter
            if run_filter:
                status.text("Step 3/7: Filtering companies...")
                progress.progress(40)

                if tools.get('company_type_filter'):
                    filtered_path = str(leads_dir / "filtered.json")
                    tools['company_type_filter'](current_path, filtered_path, config)
                    current_path = filtered_path
                    st.success("✓ Company filter complete")

            # Step 4: ICP Scoring
            if run_icp:
                status.text("Step 4/7: Scoring against ICP...")
                progress.progress(55)

                if tools.get('icp_scorer'):
                    scored_path = str(leads_dir / "scored.json")
                    tools['icp_scorer'](current_path, scored_path, config)
                    current_path = scored_path
                    st.success("✓ ICP scoring complete")

            # Step 5: Identify Decision Makers
            if run_dm:
                status.text("Step 5/7: Identifying decision makers...")
                progress.progress(70)
                # Note: identify_decision_makers works on Excel, may need adaptation
                st.info("ℹ️ Decision maker identification uses title keywords")

            # Step 6: Calculate final scores
            status.text("Step 6/7: Calculating final scores...")
            progress.progress(80)

            if tools.get('calculate_lead_score'):
                final_path = str(leads_dir / "final_scored.json")
                tools['calculate_lead_score'](current_path, final_path, config)
                current_path = final_path
                st.success("✓ Final scoring complete")

            # Step 7: Output
            status.text("Step 7/7: Generating output...")
            progress.progress(95)

            if tools.get('output_tam'):
                tools['output_tam'](current_path, config, str(output_dir))
                st.success("✓ Output generated")

            progress.progress(100)
            status.text("Pipeline complete!")

            # Show results and download
            st.markdown("---")
            st.markdown("### 📥 Download Results")

            # Find output files
            campaign_id = config.get("campaign_id", "campaign")
            excel_path = output_dir / f"{campaign_id}_tam.xlsx"
            csv_path = output_dir / f"{campaign_id}_smartlead.csv"

            col1, col2 = st.columns(2)

            with col1:
                if excel_path.exists():
                    with open(excel_path, "rb") as f:
                        st.download_button(
                            "📊 Download Excel",
                            f,
                            file_name=excel_path.name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

            with col2:
                if csv_path.exists():
                    with open(csv_path, "rb") as f:
                        st.download_button(
                            "📄 Download SmartLead CSV",
                            f,
                            file_name=csv_path.name,
                            mime="text/csv"
                        )

        except Exception as e:
            st.error(f"Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())


def page_single_lookup():
    """Single lookup tools using BlitzAPI"""
    st.title("🔍 Single Lookups")
    st.markdown("Quick lookups for individual companies or contacts.")

    tools = import_tools()

    if not tools.get('blitz_api'):
        st.error("BlitzAPI not available. Check BLITZ_API_KEY in .env")
        return

    lookup_type = st.radio(
        "What do you want to look up?",
        ["Find Decision Makers at Company", "Get Work Email for Person", "Get Phone Number", "Company Info from Domain"],
        horizontal=True
    )

    st.markdown("---")

    try:
        api = tools['blitz_api']()
    except Exception as e:
        st.error(f"Could not initialize BlitzAPI: {e}")
        return

    if lookup_type == "Find Decision Makers at Company":
        st.markdown("### Find Decision Makers")

        col1, col2 = st.columns([3, 1])
        with col1:
            company_input = st.text_input(
                "Company LinkedIn URL or Domain",
                placeholder="https://linkedin.com/company/acme OR acme.com"
            )
        with col2:
            max_results = st.number_input("Max Results", min_value=1, max_value=10, value=3)

        if st.button("🔍 Search", type="primary"):
            if not company_input:
                st.warning("Please enter a company URL or domain")
                return

            with st.spinner("Searching for decision makers..."):
                try:
                    # Determine if it's a LinkedIn URL or domain
                    if "linkedin.com" in company_input:
                        results = api.search_decision_makers(
                            company_linkedin_url=company_input,
                            max_results=max_results
                        )
                    else:
                        # First get LinkedIn URL from domain
                        company_result = api.domain_to_linkedin(company_input)
                        if company_result.found:
                            results = api.search_decision_makers(
                                company_linkedin_url=company_result.company_linkedin_url,
                                max_results=max_results
                            )
                        else:
                            st.warning(f"Could not find company LinkedIn for {company_input}")
                            return

                    if results:
                        st.success(f"Found {len(results)} decision maker(s)")

                        for i, dm in enumerate(results, 1):
                            with st.expander(f"{i}. {dm.get('full_name', 'Unknown')} - {dm.get('title', 'No title')}", expanded=True):
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.write(f"**Name:** {dm.get('full_name', 'N/A')}")
                                    st.write(f"**Title:** {dm.get('title', 'N/A')}")
                                    st.write(f"**Company:** {dm.get('company_name', 'N/A')}")
                                with col2:
                                    st.write(f"**Email:** {dm.get('email', 'N/A')}")
                                    st.write(f"**LinkedIn:** {dm.get('linkedin_url', 'N/A')}")
                    else:
                        st.info("No decision makers found")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    elif lookup_type == "Get Work Email for Person":
        st.markdown("### Get Work Email")

        linkedin_url = st.text_input(
            "Person's LinkedIn URL",
            placeholder="https://linkedin.com/in/johndoe"
        )

        if st.button("🔍 Find Email", type="primary"):
            if not linkedin_url:
                st.warning("Please enter a LinkedIn URL")
                return

            with st.spinner("Finding work email..."):
                try:
                    result = api.find_work_email(linkedin_url)

                    if result.found:
                        st.success("Email found!")
                        st.write(f"**Email:** {result.email}")

                        if result.all_emails:
                            st.write("**All emails found:**")
                            for email in result.all_emails:
                                st.write(f"  - {email.get('email')} ({email.get('type', 'unknown')})")
                    else:
                        st.warning("No email found for this person")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

    elif lookup_type == "Get Phone Number":
        st.markdown("### Get Phone Number")
        st.markdown("*Cost: 5 credits per lookup*")

        linkedin_url = st.text_input(
            "Person's LinkedIn URL",
            placeholder="https://linkedin.com/in/johndoe",
            key="phone_linkedin_url"
        )

        if st.button("📞 Find Phone", type="primary"):
            if not linkedin_url:
                st.warning("Please enter a LinkedIn URL")
            else:
                with st.spinner("Finding phone number..."):
                    try:
                        result = api.find_phone(linkedin_url)

                        if result.found:
                            st.success("Phone number found!")
                            st.write(f"**Phone:** {result.phone}")
                            if result.phone_type:
                                st.write(f"**Type:** {result.phone_type}")
                        else:
                            st.warning("No phone number found for this person")

                    except Exception as e:
                        st.error(f"Error: {str(e)}")

    elif lookup_type == "Company Info from Domain":
        st.markdown("### Company Info")

        domain = st.text_input(
            "Company Domain",
            placeholder="acme.com"
        )

        if st.button("🔍 Get Info", type="primary"):
            if not domain:
                st.warning("Please enter a domain")
                return

            with st.spinner("Getting company info..."):
                try:
                    result = api.domain_to_linkedin(domain)

                    if result.found:
                        st.success("Company found!")
                        st.write(f"**LinkedIn URL:** {result.company_linkedin_url}")
                        st.write(f"**Domain:** {result.domain}")
                    else:
                        st.warning("Company not found")

                except Exception as e:
                    st.error(f"Error: {str(e)}")


def page_identify_dm():
    """Identify Decision Makers tool"""
    st.title("👤 Identify Decision Makers")
    st.markdown("Classify contacts as decision-makers based on job titles.")

    tools = import_tools()

    uploaded_file = st.file_uploader(
        "Upload Excel file with Title column",
        type=["xlsx", "xls"],
        help="File must have a Title or Job Title column"
    )

    if uploaded_file and st.button("🚀 Run Classification", type="primary"):
        input_path = save_uploaded_file(uploaded_file)

        with st.spinner("Classifying decision makers..."):
            try:
                if tools.get('identify_decision_makers'):
                    tools['identify_decision_makers'](input_path)
                    st.success("Classification complete!")

                    # Offer download of result
                    result_path = Path(input_path)
                    with open(result_path, "rb") as f:
                        st.download_button(
                            "📥 Download Results",
                            f,
                            file_name=f"dm_{result_path.name}",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.error("Identify Decision Makers tool not available")
            except Exception as e:
                st.error(f"Error: {str(e)}")


def page_normalize_names():
    """Normalize Company Names tool"""
    st.title("✨ Normalize Company Names")
    st.markdown("Clean company names for email personalization.")
    st.markdown("Example: 'Seven Gravity Inc.' → 'Seven Gravity'")

    tools = import_tools()

    # Single name normalization
    st.markdown("### Quick Normalize (Single)")
    single_name = st.text_input("Enter company name", placeholder="Seven Gravity Inc.")

    if single_name and st.button("Normalize"):
        try:
            from normalize_company_name import normalize_company_name
            result = normalize_company_name(single_name)
            if result.success:
                st.success(f"**Result:** {result.normalized}")
            else:
                st.warning(f"Could not normalize: {result.error}")
        except Exception as e:
            st.error(f"Error: {e}")

    st.markdown("---")

    # Batch normalization
    st.markdown("### Batch Normalize (File)")
    uploaded_file = st.file_uploader(
        "Upload Excel file with Company column",
        type=["xlsx", "xls"],
        key="normalize_batch_upload",
        help="File must have a Company or Company Name column"
    )

    company_col = st.text_input(
        "Company column name",
        value="Company",
        help="The column containing company names"
    )

    if uploaded_file and st.button("🚀 Normalize All", type="primary"):
        input_path = save_uploaded_file(uploaded_file)

        try:
            # Read file
            df = pd.read_excel(input_path)

            # Find company column
            if company_col not in df.columns:
                # Try common variations
                for col in ["Company Name", "company", "company_name", "Organization"]:
                    if col in df.columns:
                        company_col_found = col
                        break
                else:
                    st.error(f"Column '{company_col}' not found. Available: {list(df.columns)}")
                    return
            else:
                company_col_found = company_col

            company_names = df[company_col_found].dropna().tolist()
            st.info(f"Normalizing {len(company_names)} company names...")

            # Import and run batch normalization
            from normalize_company_name import normalize_batch

            progress = st.progress(0)
            status = st.empty()

            # Process in smaller batches with progress
            batch_size = 50
            results = []
            total = len(company_names)

            for i in range(0, total, batch_size):
                batch = company_names[i:i+batch_size]
                batch_results = normalize_batch(batch, delay=0.5)
                results.extend(batch_results)
                progress.progress(min((i + batch_size) / total, 1.0))
                status.text(f"Processed {min(i + batch_size, total)}/{total} names...")

            # Add results to dataframe
            normalized_names = [r.normalized for r in results]
            df["Clean_Company_Name"] = None

            # Map back to original rows
            name_to_normalized = {r.original: r.normalized for r in results}
            df["Clean_Company_Name"] = df[company_col_found].apply(
                lambda x: name_to_normalized.get(x, x) if pd.notna(x) else x
            )

            # Save and offer download
            output_path = input_path.replace(".xlsx", "_normalized.xlsx").replace(".xls", "_normalized.xls")
            df.to_excel(output_path, index=False)

            st.success(f"Normalized {len(results)} company names!")

            # Show preview
            st.markdown("### Preview")
            preview_df = df[[company_col_found, "Clean_Company_Name"]].head(20)
            st.dataframe(preview_df)

            # Download button
            with open(output_path, "rb") as f:
                st.download_button(
                    "📥 Download Normalized File",
                    f,
                    file_name=os.path.basename(output_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error(f"Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())


def page_verify_emails():
    """Email verification tools"""
    st.title("✅ Email Verification")
    st.markdown("Verify emails using triple verification (BlitzAPI → MV → BB)")

    tools = import_tools()

    st.warning("⚠️ Email verification uses API credits. Use carefully.")

    verification_type = st.radio(
        "Verification Type",
        ["Triple Verify (BlitzAPI → MV → BB)", "Million Verifier Only", "Bounce Ban Only"],
        horizontal=True
    )

    # Options for triple verify
    if verification_type == "Triple Verify (BlitzAPI → MV → BB)":
        col1, col2, col3 = st.columns(3)
        with col1:
            skip_blitz = st.checkbox("Skip BlitzAPI", value=False)
        with col2:
            skip_mv = st.checkbox("Skip MillionVerifier", value=False)
        with col3:
            skip_bb = st.checkbox("Skip BounceBan", value=False)

    uploaded_file = st.file_uploader(
        "Upload file with Email column",
        type=["xlsx", "xls", "csv", "json"],
        key="verify_email_upload",
        help="File must have an Email column. For JSON, expects {leads: [...]}"
    )

    if uploaded_file and st.button("🚀 Verify Emails", type="primary"):
        input_path = save_uploaded_file(uploaded_file)
        tmp_dir = Path(__file__).parent.parent / ".tmp" / "verification"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Handle different input formats
            if uploaded_file.name.endswith(".json"):
                # Already in correct format for triple_verify_leads
                json_input_path = input_path
            else:
                # Convert Excel/CSV to JSON format
                if uploaded_file.name.endswith(".csv"):
                    df = pd.read_csv(input_path)
                else:
                    df = pd.read_excel(input_path)

                # Find email column
                email_col = None
                for col in ["Email", "email", "EMAIL", "E-mail", "e-mail"]:
                    if col in df.columns:
                        email_col = col
                        break

                if not email_col:
                    st.error(f"No email column found. Available: {list(df.columns)}")
                    return

                # Convert to leads format
                leads = []
                for _, row in df.iterrows():
                    lead = {"email": row[email_col]}
                    # Add other columns if present
                    for col in ["first_name", "First Name", "last_name", "Last Name", "company", "Company"]:
                        if col in df.columns:
                            lead[col.lower().replace(" ", "_")] = row[col]
                    leads.append(lead)

                # Save as JSON
                json_input_path = str(tmp_dir / "input_leads.json")
                with open(json_input_path, "w") as f:
                    json.dump({"leads": leads}, f)

            output_path = str(tmp_dir / "verified_leads.json")

            if verification_type == "Triple Verify (BlitzAPI → MV → BB)":
                st.info("Running triple verification... This may take several minutes.")
                progress = st.progress(0)

                try:
                    from triple_verify_emails import triple_verify_leads

                    summary = triple_verify_leads(
                        json_input_path,
                        output_path,
                        skip_blitz=skip_blitz if 'skip_blitz' in dir() else False,
                        skip_mv=skip_mv if 'skip_mv' in dir() else False,
                        skip_bb=skip_bb if 'skip_bb' in dir() else False
                    )

                    progress.progress(100)

                    # Show results
                    st.success("Verification complete!")

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Leads", summary.get("total_leads", 0))
                    with col2:
                        st.metric("Campaign Ready", summary.get("campaign_ready", 0))
                    with col3:
                        st.metric("Failed", summary.get("failed", 0))

                    # Show funnel
                    st.markdown("### Verification Funnel")
                    funnel_data = {
                        "Stage": ["Input Emails", "Pass 1 (BlitzAPI)", "Pass 2 (MV)", "Pass 3 (BB)"],
                        "Count": [
                            summary.get("leads_with_email", 0),
                            summary.get("pass_1_blitz", 0),
                            summary.get("pass_2_mv", 0),
                            summary.get("pass_3_bb", 0)
                        ]
                    }
                    st.dataframe(pd.DataFrame(funnel_data))

                    # Download results
                    with open(output_path, "r") as f:
                        verified_data = json.load(f)

                    # Convert verified leads to DataFrame for download
                    if verified_data.get("leads"):
                        verified_df = pd.DataFrame(verified_data["leads"])
                        excel_path = str(tmp_dir / "verified_leads.xlsx")
                        verified_df.to_excel(excel_path, index=False)

                        with open(excel_path, "rb") as f:
                            st.download_button(
                                "📥 Download Verified Leads (Excel)",
                                f,
                                file_name="verified_leads.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                except ImportError as e:
                    st.error(f"Required module not available: {e}")

            elif verification_type == "Million Verifier Only":
                st.info("Running Million Verifier...")

                try:
                    from millionverifier_api import verify_emails

                    # Load emails
                    with open(json_input_path, "r") as f:
                        data = json.load(f)
                    emails = [l.get("email") for l in data.get("leads", []) if l.get("email")]

                    result = verify_emails(emails, wait=True, poll_interval=10)

                    if result.get("success"):
                        st.success(f"Verified {len(emails)} emails!")

                        # Show results summary
                        results_by_email = result.get("results", {})
                        good = sum(1 for r in results_by_email.values() if r.get("quality") == "good")
                        risky = sum(1 for r in results_by_email.values() if r.get("quality") == "risky")
                        bad = sum(1 for r in results_by_email.values() if r.get("quality") == "bad")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Good", good, delta=None)
                        with col2:
                            st.metric("Risky", risky, delta=None)
                        with col3:
                            st.metric("Bad", bad, delta=None)

                        # Show details
                        results_df = pd.DataFrame([
                            {"email": email, **details}
                            for email, details in results_by_email.items()
                        ])
                        st.dataframe(results_df)
                    else:
                        st.error(f"Error: {result.get('error')}")

                except ImportError:
                    st.error("MillionVerifier API module not available")

            elif verification_type == "Bounce Ban Only":
                st.info("Running Bounce Ban...")

                try:
                    from bounceban_api import verify_emails

                    # Load emails
                    with open(json_input_path, "r") as f:
                        data = json.load(f)
                    emails = [l.get("email") for l in data.get("leads", []) if l.get("email")]

                    result = verify_emails(emails, wait=True, poll_interval=10)

                    if result.get("success"):
                        st.success(f"Verified {len(emails)} emails!")

                        # Show results
                        results_by_email = result.get("results", {})
                        deliverable = sum(1 for r in results_by_email.values() if r.get("result") == "deliverable")
                        undeliverable = sum(1 for r in results_by_email.values() if r.get("result") == "undeliverable")
                        risky = sum(1 for r in results_by_email.values() if r.get("result") == "risky")

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Deliverable", deliverable)
                        with col2:
                            st.metric("Risky", risky)
                        with col3:
                            st.metric("Undeliverable", undeliverable)

                        # Show details
                        results_df = pd.DataFrame([
                            {"email": email, **details}
                            for email, details in results_by_email.items()
                        ])
                        st.dataframe(results_df)
                    else:
                        st.error(f"Error: {result.get('error')}")

                except ImportError:
                    st.error("BounceBan API module not available")

        except Exception as e:
            st.error(f"Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())


def page_score_industries():
    """Score Industries tool"""
    st.title("📊 Score Industries")
    st.markdown("Evaluate industries for cold email lead gen viability.")
    st.markdown("""
    **Scoring Criteria:**
    - Ease of Selling (1-10): How receptive to cold email outreach
    - Ease of Fulfillment (1-10): How easy to generate quality leads
    - LTV Threshold: $10K+ lifetime value potential
    - TAM Threshold: 50K+ businesses in market
    """)

    uploaded_file = st.file_uploader(
        "Upload CSV with Industry and Sub Industry columns",
        type=["csv"],
        key="score_industries_upload",
        help="File must have 'Industry' and 'Sub Industry' columns"
    )

    if uploaded_file and st.button("🚀 Score Industries", type="primary"):
        input_path = save_uploaded_file(uploaded_file)

        try:
            # Verify columns exist
            df = pd.read_csv(input_path)
            required_cols = ["Industry", "Sub Industry"]
            missing = [c for c in required_cols if c not in df.columns]

            if missing:
                st.error(f"Missing required columns: {missing}. Available: {list(df.columns)}")
                return

            st.info("Scoring industries using GPT-4o-mini (via OpenRouter)...")

            # Import and run
            try:
                from score_industries import extract_industries_from_csv, score_industries_batch, IndustryScore, get_tier

                # Check API key
                import os
                if not os.getenv("OPENROUTER_API_KEY"):
                    st.error("OPENROUTER_API_KEY not set in environment")
                    return

                # Extract unique industries
                industries = extract_industries_from_csv(input_path)
                st.write(f"Found **{len(industries)}** unique sub-industries")

                # Process in batches
                all_scores = []
                batch_size = 12
                total_batches = (len(industries) + batch_size - 1) // batch_size

                progress = st.progress(0)
                status = st.empty()

                for i in range(0, len(industries), batch_size):
                    batch = industries[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    status.text(f"Scoring batch {batch_num}/{total_batches}...")

                    scores = score_industries_batch(batch)
                    all_scores.extend(scores)

                    progress.progress(batch_num / total_batches)

                # Sort by tier and score
                all_scores.sort(key=lambda x: (
                    {"A": 0, "B": 1, "C": 2}.get(x.tier, 3),
                    -x.total_score,
                    -x.lead_count
                ))

                st.success("Scoring complete!")

                # Show summary
                from collections import Counter
                tier_counts = Counter(s.tier for s in all_scores)

                col1, col2, col3 = st.columns(3)
                with col1:
                    tier_a = tier_counts.get("A", 0)
                    st.metric("Tier A (Prioritize)", tier_a)
                with col2:
                    tier_b = tier_counts.get("B", 0)
                    st.metric("Tier B (Include)", tier_b)
                with col3:
                    tier_c = tier_counts.get("C", 0)
                    st.metric("Tier C (Deprioritize)", tier_c)

                # Convert to DataFrame
                results_df = pd.DataFrame([
                    {
                        "Industry": s.industry,
                        "Sub Industry": s.sub_industry,
                        "Lead Count": s.lead_count,
                        "Ease of Selling": s.ease_of_selling,
                        "Ease of Fulfillment": s.ease_of_fulfillment,
                        "LTV Meets Threshold": s.ltv_meets_threshold,
                        "TAM Meets Threshold": s.tam_meets_threshold,
                        "Total Score": s.total_score,
                        "Tier": s.tier,
                        "Reasoning": s.reasoning
                    }
                    for s in all_scores
                ])

                # Show Tier A
                st.markdown("### Top Tier A Industries")
                tier_a_df = results_df[results_df["Tier"] == "A"].head(10)
                st.dataframe(tier_a_df)

                # Full results
                with st.expander("View All Results"):
                    st.dataframe(results_df)

                # Download
                output_path = input_path.replace(".csv", "_scored.csv")
                results_df.to_csv(output_path, index=False)

                with open(output_path, "rb") as f:
                    st.download_button(
                        "📥 Download Scored Industries (CSV)",
                        f,
                        file_name="scored_industries.csv",
                        mime="text/csv"
                    )

            except ImportError as e:
                st.error(f"Required module not available: {e}")

        except Exception as e:
            st.error(f"Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())


def page_categorize_niche():
    """Categorize Company Niche tool"""
    st.title("🏷️ Categorize Company Niche")
    st.markdown("Determine the primary niche of companies for targeting.")
    st.markdown("""
    **Output Format:** Business Model - Industry - Sub-specialty

    Examples:
    - B2B SaaS - HR Tech - Recruiting
    - Marketing Agency - Performance Marketing
    - E-commerce - Fashion - Sustainable
    """)

    # Single company
    st.markdown("### Quick Categorize (Single)")
    col1, col2 = st.columns(2)
    with col1:
        company_name = st.text_input("Company Name", placeholder="Acme Corp")
    with col2:
        company_website = st.text_input("Website or Description", placeholder="acme.com or brief description")

    if st.button("Categorize", key="categorize_single"):
        if not company_name and not company_website:
            st.warning("Please enter company name and/or website/description")
        else:
            try:
                from categorize_company_niche import categorize_niche

                with st.spinner("Analyzing company..."):
                    content = company_website if company_website else company_name
                    result = categorize_niche(content, company_name)

                    if result.success:
                        st.success(f"**Niche:** {result.niche}")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Confidence:** {result.confidence}")
                        with col2:
                            st.write(f"**Reasoning:** {result.reasoning}")
                    else:
                        st.error(f"Error: {result.error}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("---")

    # Batch categorization
    st.markdown("### Batch Categorize (File)")
    st.markdown("Upload an Excel file with company names and descriptions/websites to categorize multiple companies.")

    uploaded_file = st.file_uploader(
        "Upload Excel file",
        type=["xlsx", "xls"],
        key="categorize_batch_upload",
        help="Should have Company Name and optionally Website or Description columns"
    )

    if uploaded_file:
        col1, col2 = st.columns(2)
        with col1:
            name_col = st.text_input("Company Name column", value="Company", key="niche_name_col")
        with col2:
            content_col = st.text_input("Website/Description column", value="Website", key="niche_content_col",
                                        help="Leave empty to use company name only")

        if st.button("🚀 Categorize All", type="primary"):
            input_path = save_uploaded_file(uploaded_file)

            try:
                df = pd.read_excel(input_path)

                # Find columns
                if name_col not in df.columns:
                    st.error(f"Column '{name_col}' not found. Available: {list(df.columns)}")
                    return

                from categorize_company_niche import categorize_niche

                # Prepare data
                companies = []
                for _, row in df.iterrows():
                    company_name = row.get(name_col, "")
                    content = row.get(content_col, "") if content_col and content_col in df.columns else ""
                    if not content:
                        content = company_name
                    companies.append((company_name, content))

                # Process
                st.info(f"Categorizing {len(companies)} companies... This may take a while.")
                progress = st.progress(0)
                status = st.empty()

                results = []
                for i, (name, content) in enumerate(companies):
                    status.text(f"Processing {i+1}/{len(companies)}: {name[:30]}...")

                    try:
                        result = categorize_niche(content, name)
                        results.append({
                            "company_name": name,
                            "niche": result.niche,
                            "confidence": result.confidence,
                            "reasoning": result.reasoning
                        })
                    except Exception as e:
                        results.append({
                            "company_name": name,
                            "niche": "Error",
                            "confidence": "Low",
                            "reasoning": str(e)
                        })

                    progress.progress((i + 1) / len(companies))

                    # Small delay to avoid rate limits
                    time.sleep(0.5)

                # Add to dataframe
                df["Verified_Niche"] = [r["niche"] for r in results]
                df["Niche_Confidence"] = [r["confidence"] for r in results]
                df["Niche_Reasoning"] = [r["reasoning"] for r in results]

                st.success(f"Categorized {len(results)} companies!")

                # Show summary
                from collections import Counter
                niche_counts = Counter(r["niche"] for r in results)
                st.markdown("### Top Niches Found")
                niche_df = pd.DataFrame([
                    {"Niche": n, "Count": c}
                    for n, c in niche_counts.most_common(10)
                ])
                st.dataframe(niche_df)

                # Preview
                with st.expander("View Results"):
                    st.dataframe(df[[name_col, "Verified_Niche", "Niche_Confidence"]].head(50))

                # Download
                output_path = input_path.replace(".xlsx", "_niches.xlsx").replace(".xls", "_niches.xls")
                df.to_excel(output_path, index=False)

                with open(output_path, "rb") as f:
                    st.download_button(
                        "📥 Download Results (Excel)",
                        f,
                        file_name=os.path.basename(output_path),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())


# ============== MAIN ==============

def main():
    # Password check
    if not check_password():
        return

    # Sidebar navigation
    st.sidebar.title("🚀 Navigation")

    # Core Tools
    st.sidebar.markdown("### Core Tools")
    page = st.sidebar.radio(
        "Select Tool",
        [
            "🏠 Home",
            "📋 Lead Pipeline",
            "🔍 Single Lookups",
            "👤 Identify Decision Makers",
            "✨ Normalize Names",
            "✅ Verify Emails"
        ],
        label_visibility="collapsed"
    )

    # Advanced/Strategic Tools
    st.sidebar.markdown("### Advanced")
    advanced_page = st.sidebar.radio(
        "Strategic Tools",
        [
            "None",
            "🏷️ Categorize Niche",
            "📊 Score Industries"
        ],
        label_visibility="collapsed"
    )

    # If advanced tool selected, override page
    if advanced_page != "None":
        page = advanced_page

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Seven Gravity Automation Hub**")
    st.sidebar.markdown(f"v1.0 | {datetime.now().strftime('%Y-%m-%d')}")

    # Route to page
    if page == "🏠 Home":
        page_home()
    elif page == "📋 Lead Pipeline":
        page_lead_pipeline()
    elif page == "🔍 Single Lookups":
        page_single_lookup()
    elif page == "👤 Identify Decision Makers":
        page_identify_dm()
    elif page == "✨ Normalize Names":
        page_normalize_names()
    elif page == "🏷️ Categorize Niche":
        page_categorize_niche()
    elif page == "📊 Score Industries":
        page_score_industries()
    elif page == "✅ Verify Emails":
        page_verify_emails()


if __name__ == "__main__":
    main()
