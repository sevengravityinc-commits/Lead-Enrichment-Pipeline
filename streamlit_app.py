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
    """Normalize Company Names tool - with checkpointing for large files"""
    st.title("✨ Normalize Company Names")
    st.markdown("Clean company names for email personalization.")
    st.markdown("Example: 'Seven Gravity Inc.' → 'Seven Gravity'")

    tools = import_tools()

    # Initialize session state for checkpointing
    if 'normalize_checkpoint_data' not in st.session_state:
        st.session_state.normalize_checkpoint_data = None
    if 'normalize_resume_mode' not in st.session_state:
        st.session_state.normalize_resume_mode = False
    if 'normalize_cancel_requested' not in st.session_state:
        st.session_state.normalize_cancel_requested = False

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

    if uploaded_file:
        input_path = save_uploaded_file(uploaded_file)

        try:
            # Read file
            df = pd.read_excel(input_path)

            # Find company column
            if company_col not in df.columns:
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
            st.write(f"**Found {len(company_names):,} company names to normalize**")

            # Check for existing checkpoint
            try:
                from categorize_company_niche import (
                    get_file_hash, get_checkpoint_path, load_checkpoint,
                    save_checkpoint, delete_checkpoint
                )

                file_hash = get_file_hash(input_path)
                checkpoint_path = get_checkpoint_path(f"normalize_{file_hash}")
                existing_checkpoint = load_checkpoint(checkpoint_path)

                if existing_checkpoint:
                    processed_count = len(existing_checkpoint.get('results', []))
                    total_count = existing_checkpoint.get('total', 0)
                    last_updated = existing_checkpoint.get('last_updated', 'Unknown')

                    st.warning(f"⏸️ **Found checkpoint**: {processed_count:,}/{total_count:,} normalized (last: {last_updated})")

                    col_resume, col_fresh, col_clear = st.columns([2, 2, 1])
                    with col_resume:
                        if st.button("▶️ Resume", key="normalize_resume"):
                            st.session_state.normalize_checkpoint_data = existing_checkpoint
                            st.session_state.normalize_resume_mode = True
                    with col_fresh:
                        if st.button("🔄 Start fresh", key="normalize_fresh"):
                            delete_checkpoint(checkpoint_path)
                            st.session_state.normalize_checkpoint_data = None
                            st.session_state.normalize_resume_mode = False
                            st.rerun()
                    with col_clear:
                        if st.button("🗑️", key="normalize_clear"):
                            delete_checkpoint(checkpoint_path)
                            st.rerun()
            except ImportError:
                file_hash = None
                checkpoint_path = None

            # Start/Cancel buttons
            col1, col2 = st.columns([3, 1])
            with col1:
                btn_label = "▶️ Continue" if st.session_state.normalize_resume_mode else "🚀 Normalize All"
                start_button = st.button(btn_label, type="primary", key="normalize_start")
            with col2:
                if st.button("🛑 Cancel", key="normalize_cancel"):
                    st.session_state.normalize_cancel_requested = True

            if start_button:
                st.session_state.normalize_cancel_requested = False

                try:
                    from normalize_company_name import normalize_batch
                    from categorize_company_niche import save_checkpoint, delete_checkpoint
                    from datetime import datetime

                    # Initialize from checkpoint if resuming
                    if st.session_state.normalize_resume_mode and st.session_state.normalize_checkpoint_data:
                        results_data = st.session_state.normalize_checkpoint_data.get('results', [])
                        processed_names = set(st.session_state.normalize_checkpoint_data.get('processed_names', []))
                        st.info(f"▶️ Resuming: {len(processed_names):,} already done")
                    else:
                        results_data = []
                        processed_names = set()

                    progress = st.progress(len(processed_names) / len(company_names) if company_names else 0)
                    status = st.empty()

                    batch_size = 50
                    total = len(company_names)
                    checkpoint_interval = 5
                    batches_since_checkpoint = 0

                    checkpoint_data = {
                        'file_hash': file_hash,
                        'file_name': uploaded_file.name,
                        'total': total,
                        'results': results_data,
                        'processed_names': list(processed_names),
                        'started_at': st.session_state.normalize_checkpoint_data.get('started_at', datetime.now().isoformat()) if st.session_state.normalize_checkpoint_data else datetime.now().isoformat()
                    }

                    for i in range(0, total, batch_size):
                        if st.session_state.normalize_cancel_requested:
                            checkpoint_data['results'] = results_data
                            checkpoint_data['processed_names'] = list(processed_names)
                            save_checkpoint(checkpoint_path, checkpoint_data)
                            st.warning(f"⏸️ Paused at {len(processed_names):,}/{total:,}. Progress saved!")
                            break

                        # Get names not yet processed
                        batch_names = [n for n in company_names[i:i+batch_size] if n not in processed_names]
                        if not batch_names:
                            continue

                        status.text(f"Processing batch {i//batch_size + 1}... ({len(processed_names):,}/{total:,})")

                        batch_results = normalize_batch(batch_names, delay=0.5)
                        for r in batch_results:
                            results_data.append({'original': r.original, 'normalized': r.normalized})
                            processed_names.add(r.original)

                        batches_since_checkpoint += 1
                        if batches_since_checkpoint >= checkpoint_interval:
                            checkpoint_data['results'] = results_data
                            checkpoint_data['processed_names'] = list(processed_names)
                            save_checkpoint(checkpoint_path, checkpoint_data)
                            batches_since_checkpoint = 0

                        progress.progress(len(processed_names) / total)

                    # Processing complete
                    status.empty()
                    st.session_state.normalize_resume_mode = False
                    st.session_state.normalize_checkpoint_data = None

                    if len(processed_names) >= total:
                        delete_checkpoint(checkpoint_path)

                    if results_data:
                        # Map results back to dataframe
                        name_to_normalized = {r['original']: r['normalized'] for r in results_data}
                        df["Clean_Company_Name"] = df[company_col_found].apply(
                            lambda x: name_to_normalized.get(x, x) if pd.notna(x) else x
                        )

                        import os.path
                        base_name, ext = os.path.splitext(input_path)
                        output_path = f"{base_name}_normalized.xlsx"
                        df.to_excel(output_path, index=False, engine='openpyxl')

                        st.success(f"✅ Normalized {len(results_data):,} company names!")

                        st.markdown("### Preview")
                        preview_df = df[[company_col_found, "Clean_Company_Name"]].head(20)
                        st.dataframe(preview_df)

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
    st.info("💡 **Tip**: For large files (500+ emails), process in batches of 200-300 to avoid timeouts.")

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
                        verified_df.to_excel(excel_path, index=False, engine='openpyxl')

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
    """Score Industries tool - with checkpointing"""
    st.title("📊 Score Industries")
    st.markdown("Evaluate industries for cold email lead gen viability.")
    st.markdown("""
    **Scoring Criteria:**
    - Ease of Selling (1-10): How receptive to cold email outreach
    - Ease of Fulfillment (1-10): How easy to generate quality leads
    - LTV Threshold: $10K+ lifetime value potential
    - TAM Threshold: 50K+ businesses in market
    """)

    # Initialize session state for checkpointing
    if 'score_checkpoint_data' not in st.session_state:
        st.session_state.score_checkpoint_data = None
    if 'score_resume_mode' not in st.session_state:
        st.session_state.score_resume_mode = False
    if 'score_cancel_requested' not in st.session_state:
        st.session_state.score_cancel_requested = False

    uploaded_file = st.file_uploader(
        "Upload CSV with Industry and Sub Industry columns",
        type=["csv"],
        key="score_industries_upload",
        help="File must have 'Industry' and 'Sub Industry' columns"
    )

    if uploaded_file:
        input_path = save_uploaded_file(uploaded_file)

        try:
            # Verify columns exist
            df = pd.read_csv(input_path)
            required_cols = ["Industry", "Sub Industry"]
            missing = [c for c in required_cols if c not in df.columns]

            if missing:
                st.error(f"Missing required columns: {missing}. Available: {list(df.columns)}")
                return

            # Check for existing checkpoint
            try:
                from categorize_company_niche import (
                    get_file_hash, get_checkpoint_path, load_checkpoint,
                    save_checkpoint, delete_checkpoint
                )

                file_hash = get_file_hash(input_path)
                checkpoint_path = get_checkpoint_path(f"score_{file_hash}")
                existing_checkpoint = load_checkpoint(checkpoint_path)

                if existing_checkpoint:
                    processed_count = len(existing_checkpoint.get('scored_industries', []))
                    total_count = existing_checkpoint.get('total', 0)
                    last_updated = existing_checkpoint.get('last_updated', 'Unknown')

                    st.warning(f"⏸️ **Found checkpoint**: {processed_count}/{total_count} scored (last: {last_updated})")

                    col_resume, col_fresh, col_clear = st.columns([2, 2, 1])
                    with col_resume:
                        if st.button("▶️ Resume", key="score_resume"):
                            st.session_state.score_checkpoint_data = existing_checkpoint
                            st.session_state.score_resume_mode = True
                    with col_fresh:
                        if st.button("🔄 Start fresh", key="score_fresh"):
                            delete_checkpoint(checkpoint_path)
                            st.session_state.score_checkpoint_data = None
                            st.session_state.score_resume_mode = False
                            st.rerun()
                    with col_clear:
                        if st.button("🗑️", key="score_clear"):
                            delete_checkpoint(checkpoint_path)
                            st.rerun()
            except ImportError:
                file_hash = None
                checkpoint_path = None

            # Start/Cancel buttons
            col1, col2 = st.columns([3, 1])
            with col1:
                btn_label = "▶️ Continue" if st.session_state.score_resume_mode else "🚀 Score Industries"
                start_button = st.button(btn_label, type="primary", key="score_start")
            with col2:
                if st.button("🛑 Cancel", key="score_cancel"):
                    st.session_state.score_cancel_requested = True

            if start_button:
                st.session_state.score_cancel_requested = False
                st.info("Scoring industries using GPT-4o-mini (via OpenRouter)...")

                try:
                    from score_industries import extract_industries_from_csv, score_industries_batch, IndustryScore, get_tier
                    from categorize_company_niche import save_checkpoint, delete_checkpoint
                    from datetime import datetime

                    # Check API key
                    import os
                    if not os.getenv("OPENROUTER_API_KEY"):
                        st.error("OPENROUTER_API_KEY not set in environment")
                        return

                    # Extract unique industries
                    industries = extract_industries_from_csv(input_path)
                    st.write(f"Found **{len(industries)}** unique sub-industries")

                    # Initialize from checkpoint if resuming
                    if st.session_state.score_resume_mode and st.session_state.score_checkpoint_data:
                        scored_data = st.session_state.score_checkpoint_data.get('scored_industries', [])
                        processed_keys = set(st.session_state.score_checkpoint_data.get('processed_keys', []))
                        st.info(f"▶️ Resuming: {len(processed_keys)} already scored")
                    else:
                        scored_data = []
                        processed_keys = set()

                    # Process in batches
                    batch_size = 12
                    total_batches = (len(industries) + batch_size - 1) // batch_size
                    checkpoint_interval = 3
                    batches_since_checkpoint = 0

                    checkpoint_data = {
                        'file_hash': file_hash,
                        'file_name': uploaded_file.name,
                        'total': len(industries),
                        'scored_industries': scored_data,
                        'processed_keys': list(processed_keys),
                        'started_at': st.session_state.score_checkpoint_data.get('started_at', datetime.now().isoformat()) if st.session_state.score_checkpoint_data else datetime.now().isoformat()
                    }

                    progress = st.progress(len(processed_keys) / len(industries) if industries else 0)
                    status = st.empty()

                    for i in range(0, len(industries), batch_size):
                        if st.session_state.score_cancel_requested:
                            checkpoint_data['scored_industries'] = scored_data
                            checkpoint_data['processed_keys'] = list(processed_keys)
                            save_checkpoint(checkpoint_path, checkpoint_data)
                            st.warning(f"⏸️ Paused at {len(processed_keys)}/{len(industries)}. Progress saved!")
                            break

                        # Get industries not yet processed
                        batch = [ind for ind in industries[i:i + batch_size]
                                 if f"{ind.industry}|{ind.sub_industry}" not in processed_keys]

                        if not batch:
                            continue

                        batch_num = i // batch_size + 1
                        status.text(f"Scoring batch {batch_num}/{total_batches}... ({len(processed_keys)}/{len(industries)})")

                        scores = score_industries_batch(batch)
                        for s in scores:
                            key = f"{s.industry}|{s.sub_industry}"
                            processed_keys.add(key)
                            scored_data.append({
                                'industry': s.industry,
                                'sub_industry': s.sub_industry,
                                'lead_count': s.lead_count,
                                'ease_of_selling': s.ease_of_selling,
                                'ease_of_fulfillment': s.ease_of_fulfillment,
                                'ltv_meets_threshold': s.ltv_meets_threshold,
                                'tam_meets_threshold': s.tam_meets_threshold,
                                'total_score': s.total_score,
                                'tier': s.tier,
                                'reasoning': s.reasoning
                            })

                        batches_since_checkpoint += 1
                        if batches_since_checkpoint >= checkpoint_interval:
                            checkpoint_data['scored_industries'] = scored_data
                            checkpoint_data['processed_keys'] = list(processed_keys)
                            save_checkpoint(checkpoint_path, checkpoint_data)
                            batches_since_checkpoint = 0

                        progress.progress(len(processed_keys) / len(industries))

                    # Processing complete
                    status.empty()
                    st.session_state.score_resume_mode = False
                    st.session_state.score_checkpoint_data = None

                    if len(processed_keys) >= len(industries):
                        delete_checkpoint(checkpoint_path)

                    if scored_data:
                        # Sort by tier and score
                        scored_data.sort(key=lambda x: (
                            {"A": 0, "B": 1, "C": 2}.get(x['tier'], 3),
                            -x['total_score'],
                            -x['lead_count']
                        ))

                        st.success(f"✅ Scored {len(scored_data)} industries!")

                        # Show summary
                        from collections import Counter
                        tier_counts = Counter(s['tier'] for s in scored_data)

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
                                "Industry": s['industry'],
                                "Sub Industry": s['sub_industry'],
                                "Lead Count": s['lead_count'],
                                "Ease of Selling": s['ease_of_selling'],
                                "Ease of Fulfillment": s['ease_of_fulfillment'],
                                "LTV Meets Threshold": s['ltv_meets_threshold'],
                                "TAM Meets Threshold": s['tam_meets_threshold'],
                                "Total Score": s['total_score'],
                                "Tier": s['tier'],
                                "Reasoning": s['reasoning']
                            }
                            for s in scored_data
                        ])

                        # Show Tier A
                        st.markdown("### Top Tier A Industries")
                        tier_a_df = results_df[results_df["Tier"] == "A"].head(10)
                        st.dataframe(tier_a_df)

                        # Full results
                        with st.expander("View All Results"):
                            st.dataframe(results_df)

                        # Download
                        import os.path
                        base_name, ext = os.path.splitext(input_path)
                        output_path = f"{base_name}_scored.csv"
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
    """Categorize Company Niche tool - Enhanced with batch mode and checkpointing"""
    st.title("🏷️ Categorize Company Niche")
    st.markdown("Determine the primary niche of companies for targeting.")

    # Initialize session state for cancel and checkpointing
    if 'niche_cancel_requested' not in st.session_state:
        st.session_state.niche_cancel_requested = False
    if 'niche_processing' not in st.session_state:
        st.session_state.niche_processing = False
    if 'niche_checkpoint_data' not in st.session_state:
        st.session_state.niche_checkpoint_data = None
    if 'niche_resume_mode' not in st.session_state:
        st.session_state.niche_resume_mode = False

    # Mode selection
    st.markdown("### Choose Mode")
    mode = st.radio(
        "Categorization Mode",
        ["🎯 Classify into my niches", "🔍 Discover niches (AI decides)"],
        horizontal=True,
        help="Classify: You provide target niches. Discover: AI identifies niches automatically."
    )

    # Predefined niches input (only for Classify mode)
    predefined_niches = None
    if "Classify" in mode:
        st.markdown("#### Your Target Niches")
        st.markdown("*Enter one niche per line. AI will classify companies into these (with fuzzy matching).*")
        niches_text = st.text_area(
            "Target Niches",
            value="",
            height=120,
            key="target_niches_input",
            placeholder="Marketing, Advertising & PR\nB2B SaaS\nE-commerce\nHealthcare",
            help="Companies that don't match will be labeled 'Other - [AI suggestion]'"
        )
        predefined_niches = [n.strip() for n in niches_text.strip().split("\n") if n.strip()]

        if predefined_niches:
            st.info(f"Will classify into {len(predefined_niches)} niches: {', '.join(predefined_niches)}")
        else:
            st.warning("Enter at least one target niche above, or switch to 'Discover' mode.")
    else:
        st.info("AI will discover and group similar companies into niches automatically.")

    st.markdown("---")

    # Single company quick test
    st.markdown("### Quick Test (Single Company)")
    col1, col2 = st.columns(2)
    with col1:
        company_name = st.text_input("Company Name", placeholder="Acme Corp", key="single_company_name")
    with col2:
        company_website = st.text_input("Website or Description", placeholder="acme.com or brief description", key="single_company_content")

    if st.button("🏷️ Categorize", key="categorize_single"):
        if not company_name and not company_website:
            st.warning("Please enter company name and/or website/description")
        else:
            try:
                from categorize_company_niche import categorize_niche, categorize_niche_batch

                with st.spinner("Analyzing company..."):
                    content = company_website if company_website else company_name

                    if predefined_niches:
                        # Use batch function even for single to test classify mode
                        results = categorize_niche_batch(
                            [{"name": company_name, "content": content}],
                            predefined_niches=predefined_niches,
                            batch_size=1
                        )
                        if results:
                            r = results[0]
                            st.success(f"**Niche:** {r.get('niche', 'Unknown')}")
                            st.write(f"**Match Type:** {r.get('match_type', 'unknown')}")
                    else:
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
    st.markdown("Upload an Excel/CSV file with company names. Existing Industry/Sub-Industry columns will be ignored.")

    uploaded_file = st.file_uploader(
        "Upload Excel or CSV file",
        type=["xlsx", "xls", "csv"],
        key="categorize_batch_upload",
        help="Should have Company Name and optionally Website or Description columns"
    )

    if uploaded_file:
        # Load file to show preview
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.write(f"**Loaded {len(df):,} rows** | Columns: {', '.join(df.columns[:10])}")

            # Column selection
            col1, col2 = st.columns(2)
            with col1:
                name_col = st.selectbox(
                    "Company Name column",
                    options=df.columns.tolist(),
                    index=0 if "Company" not in df.columns else df.columns.tolist().index("Company") if "Company" in df.columns else 0,
                    key="niche_name_col"
                )
            with col2:
                content_options = ["(Use company name only)"] + df.columns.tolist()
                content_col = st.selectbox(
                    "Website/Description column (optional)",
                    options=content_options,
                    index=0,
                    key="niche_content_col"
                )

            # Processing mode info
            batch_threshold = 500
            use_batching = len(df) >= batch_threshold
            # Use larger batches for big files (faster but slightly less accurate)
            if len(df) > 1000:
                batch_size = 50  # ~92-95% accuracy, 2x faster
            elif use_batching:
                batch_size = 20  # ~95-98% accuracy
            else:
                batch_size = 1   # Precision mode

            if use_batching:
                speed_note = " (optimized for speed)" if batch_size == 50 else ""
                st.info(f"📦 **Batch Mode**: {len(df):,} records in batches of {batch_size}{speed_note} (~{len(df) // batch_size + 1} API calls)")
            else:
                st.info(f"🎯 **Precision Mode**: {len(df)} records will be processed one at a time for maximum accuracy")

            # Check for existing checkpoint
            try:
                from categorize_company_niche import (
                    get_file_hash, get_checkpoint_path, load_checkpoint,
                    save_checkpoint, delete_checkpoint
                )

                # Save file temporarily to get hash
                temp_input_path = save_uploaded_file(uploaded_file)
                file_hash = get_file_hash(temp_input_path)
                checkpoint_path = get_checkpoint_path(file_hash)
                existing_checkpoint = load_checkpoint(checkpoint_path)

                if existing_checkpoint and not st.session_state.niche_processing:
                    processed_count = len(existing_checkpoint.get('processed_indices', []))
                    total_count = existing_checkpoint.get('total_rows', 0)
                    last_updated = existing_checkpoint.get('last_updated', 'Unknown')

                    st.warning(f"⏸️ **Found checkpoint**: {processed_count:,}/{total_count:,} rows processed (last updated: {last_updated})")

                    col_resume, col_fresh, col_clear = st.columns([2, 2, 1])
                    with col_resume:
                        resume_button = st.button("▶️ Resume from checkpoint", type="primary")
                    with col_fresh:
                        fresh_button = st.button("🔄 Start fresh")
                    with col_clear:
                        clear_button = st.button("🗑️ Clear")

                    if clear_button:
                        delete_checkpoint(checkpoint_path)
                        st.session_state.niche_checkpoint_data = None
                        st.session_state.niche_resume_mode = False
                        st.rerun()

                    if resume_button:
                        st.session_state.niche_checkpoint_data = existing_checkpoint
                        st.session_state.niche_resume_mode = True

                    if fresh_button:
                        delete_checkpoint(checkpoint_path)
                        st.session_state.niche_checkpoint_data = None
                        st.session_state.niche_resume_mode = False
                else:
                    st.session_state.niche_checkpoint_data = None
                    st.session_state.niche_resume_mode = False

            except ImportError:
                # Checkpoint functions not available, continue without
                st.session_state.niche_checkpoint_data = None
                st.session_state.niche_resume_mode = False
                file_hash = None
                checkpoint_path = None

            # Start/Cancel buttons
            col1, col2 = st.columns([3, 1])
            with col1:
                # Show different button text based on resume mode
                if st.session_state.niche_resume_mode:
                    start_button = st.button("▶️ Continue Processing", type="primary", disabled=st.session_state.niche_processing)
                else:
                    start_button = st.button("🚀 Start Categorization", type="primary", disabled=st.session_state.niche_processing)
            with col2:
                if st.session_state.niche_processing:
                    if st.button("🛑 Cancel", type="secondary"):
                        st.session_state.niche_cancel_requested = True
                        st.warning("Cancellation requested... will stop after current batch.")

            if start_button:
                st.session_state.niche_processing = True
                st.session_state.niche_cancel_requested = False

                # Save file for processing
                input_path = save_uploaded_file(uploaded_file)
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(input_path)
                else:
                    df = pd.read_excel(input_path)

                try:
                    from categorize_company_niche import categorize_niche, categorize_niche_batch
                    from categorize_company_niche import (
                        get_file_hash, get_checkpoint_path, save_checkpoint, delete_checkpoint
                    )
                    from datetime import datetime

                    # Get checkpoint path for this file
                    file_hash = get_file_hash(input_path)
                    checkpoint_path = get_checkpoint_path(file_hash)

                    # Prepare companies list
                    companies = []
                    for _, row in df.iterrows():
                        name = str(row.get(name_col, "")).strip()
                        if content_col and content_col != "(Use company name only)" and content_col in df.columns:
                            content = str(row.get(content_col, "")).strip()
                        else:
                            content = name
                        companies.append({"name": name, "content": content})

                    total = len(companies)

                    # Initialize from checkpoint if resuming
                    if st.session_state.niche_resume_mode and st.session_state.niche_checkpoint_data:
                        results = st.session_state.niche_checkpoint_data.get('results', [])
                        processed_indices = set(st.session_state.niche_checkpoint_data.get('processed_indices', []))
                        st.info(f"▶️ Resuming: {len(processed_indices):,}/{total:,} already done, {total - len(processed_indices):,} remaining")
                    else:
                        results = []
                        processed_indices = set()
                        st.info(f"Processing {total:,} companies...")

                    # Progress tracking
                    initial_progress = len(processed_indices) / total if total > 0 else 0
                    progress_bar = st.progress(initial_progress)
                    status_text = st.empty()
                    eta_text = st.empty()

                    start_time = time.time()
                    checkpoint_interval = 5  # Save every 5 batches
                    batches_since_checkpoint = 0

                    # Create checkpoint data structure
                    checkpoint_data = {
                        'file_hash': file_hash,
                        'file_name': uploaded_file.name,
                        'total_rows': total,
                        'processed_indices': list(processed_indices),
                        'results': results,
                        'predefined_niches': predefined_niches,
                        'mode': 'classify' if predefined_niches else 'discover',
                        'batch_size': batch_size,
                        'started_at': st.session_state.niche_checkpoint_data.get('started_at', datetime.now().isoformat()) if st.session_state.niche_checkpoint_data else datetime.now().isoformat()
                    }

                    if use_batching:
                        # Batch mode
                        num_batches = (total + batch_size - 1) // batch_size

                        for batch_idx in range(num_batches):
                            # Check for cancellation
                            if st.session_state.niche_cancel_requested:
                                # Save checkpoint before stopping
                                checkpoint_data['processed_indices'] = list(processed_indices)
                                checkpoint_data['results'] = results
                                save_checkpoint(checkpoint_path, checkpoint_data)
                                st.warning(f"⏸️ Paused at {len(results):,}/{total:,}. Progress saved - resume anytime!")
                                break

                            batch_start = batch_idx * batch_size
                            batch_end = min(batch_start + batch_size, total)

                            # Skip batches that are already fully processed
                            batch_indices = set(range(batch_start, batch_end))
                            if batch_indices.issubset(processed_indices):
                                continue

                            # Get companies that haven't been processed yet
                            batch_to_process = []
                            batch_indices_to_process = []
                            for i in range(batch_start, batch_end):
                                if i not in processed_indices:
                                    batch_to_process.append(companies[i])
                                    batch_indices_to_process.append(i)

                            if not batch_to_process:
                                continue

                            status_text.text(f"Batch {batch_idx + 1}/{num_batches} | Processing {len(batch_to_process)} companies")

                            batch_results = categorize_niche_batch(
                                batch_to_process,
                                predefined_niches=predefined_niches,
                                batch_size=batch_size
                            )
                            # Adjust indices to global position
                            for i, r in enumerate(batch_results):
                                r["index"] = batch_indices_to_process[i]
                                processed_indices.add(batch_indices_to_process[i])
                            results.extend(batch_results)

                            batches_since_checkpoint += 1

                            # Save checkpoint every N batches
                            if batches_since_checkpoint >= checkpoint_interval:
                                checkpoint_data['processed_indices'] = list(processed_indices)
                                checkpoint_data['results'] = results
                                save_checkpoint(checkpoint_path, checkpoint_data)
                                batches_since_checkpoint = 0

                            # Update progress
                            progress = len(processed_indices) / total
                            progress_bar.progress(progress)

                            # Calculate ETA
                            elapsed = time.time() - start_time
                            if len(results) > 0:
                                rate = len(results) / elapsed
                                remaining = total - len(results)
                                eta_seconds = remaining / rate if rate > 0 else 0
                                eta_text.text(f"⏱️ {int(progress * 100)}% | ~{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s remaining")

                    else:
                        # Single mode (more accurate)
                        items_since_checkpoint = 0
                        for i, company in enumerate(companies):
                            # Skip already processed items
                            if i in processed_indices:
                                continue

                            if st.session_state.niche_cancel_requested:
                                # Save checkpoint before stopping
                                checkpoint_data['processed_indices'] = list(processed_indices)
                                checkpoint_data['results'] = results
                                save_checkpoint(checkpoint_path, checkpoint_data)
                                st.warning(f"⏸️ Paused at {len(results):,}/{total:,}. Progress saved - resume anytime!")
                                break

                            status_text.text(f"Processing {len(processed_indices) + 1}/{total}: {company['name'][:40]}...")

                            if predefined_niches:
                                batch_results = categorize_niche_batch(
                                    [company],
                                    predefined_niches=predefined_niches,
                                    batch_size=1
                                )
                                if batch_results:
                                    batch_results[0]["index"] = i
                                    results.append(batch_results[0])
                            else:
                                result = categorize_niche(company['content'], company['name'])
                                results.append({
                                    "index": i,
                                    "company": company['name'],
                                    "niche": result.niche,
                                    "match_type": "single",
                                    "confidence": result.confidence
                                })

                            processed_indices.add(i)
                            items_since_checkpoint += 1

                            # Save checkpoint every 100 items in single mode
                            if items_since_checkpoint >= 100:
                                checkpoint_data['processed_indices'] = list(processed_indices)
                                checkpoint_data['results'] = results
                                save_checkpoint(checkpoint_path, checkpoint_data)
                                items_since_checkpoint = 0

                            progress_bar.progress(len(processed_indices) / total)

                            # ETA calculation
                            elapsed = time.time() - start_time
                            rate = len(processed_indices) / elapsed if elapsed > 0 else 1
                            remaining = total - len(processed_indices)
                            eta_seconds = remaining / rate if rate > 0 else 0
                            eta_text.text(f"⏱️ {int(len(processed_indices) / total * 100)}% | ~{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s remaining")

                            time.sleep(0.3)  # Small delay for rate limits

                    # Processing complete
                    st.session_state.niche_processing = False
                    st.session_state.niche_resume_mode = False
                    st.session_state.niche_checkpoint_data = None
                    status_text.empty()
                    eta_text.empty()

                    # Delete checkpoint on successful completion (all items processed)
                    if len(processed_indices) >= total and not st.session_state.niche_cancel_requested:
                        delete_checkpoint(checkpoint_path)

                    if results:
                        # Add results to dataframe
                        # Create a mapping from index to result
                        result_map = {r.get("index", i): r for i, r in enumerate(results)}

                        niches = []
                        match_types = []
                        for i in range(len(df)):
                            r = result_map.get(i, {"niche": "Not processed", "match_type": "skipped"})
                            niches.append(r.get("niche", "Unknown"))
                            match_types.append(r.get("match_type", "unknown"))

                        df["AI_Niche"] = niches
                        df["Match_Type"] = match_types

                        st.success(f"✅ Categorized {len(results):,} companies!")

                        # Show summary
                        from collections import Counter
                        niche_counts = Counter(r.get("niche", "Unknown") for r in results)

                        st.markdown("### 📊 Niche Distribution")
                        summary_data = []
                        for niche, count in niche_counts.most_common(20):
                            pct = count / len(results) * 100
                            summary_data.append({"Niche": niche, "Count": count, "Percentage": f"{pct:.1f}%"})

                        summary_df = pd.DataFrame(summary_data)
                        st.dataframe(summary_df, use_container_width=True)

                        # Show match type breakdown
                        if predefined_niches:
                            match_counts = Counter(r.get("match_type", "unknown") for r in results)
                            st.markdown("### Match Type Breakdown")
                            match_data = [{"Type": t, "Count": c} for t, c in match_counts.items()]
                            st.dataframe(pd.DataFrame(match_data))

                        # Preview results - show ALL columns (original + new)
                        with st.expander("📋 View Results (first 100 rows)"):
                            st.dataframe(df.head(100), use_container_width=True)

                        # Download - properly handle file extension
                        import os.path
                        base_name, ext = os.path.splitext(input_path)
                        output_path = f"{base_name}_niches.xlsx"
                        df.to_excel(output_path, index=False, engine='openpyxl')

                        with open(output_path, "rb") as f:
                            st.download_button(
                                "📥 Download Results (Excel)",
                                f,
                                file_name=os.path.basename(output_path),
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                except Exception as e:
                    st.session_state.niche_processing = False
                    st.error(f"Error: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

        except Exception as e:
            st.error(f"Error loading file: {str(e)}")


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
