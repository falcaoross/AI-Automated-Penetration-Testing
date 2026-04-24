import streamlit as st
import os
import subprocess
import glob
import json
import time

st.set_page_config(page_title="AI Pentest Engine", layout="wide", page_icon="⚡")

# Custom CSS for Modern, Classy UI
st.markdown("""
<style>
    :root {
        --primary-color: #00d2ff;
        --bg-color: #0d1117;
        --panel-bg: #161b22;
        --text-color: #c9d1d9;
    }
    .stApp {
        background-color: var(--bg-color);
        color: var(--text-color);
    }
    .main-header {
        font-family: 'Inter', sans-serif;
        font-weight: 800;
        font-size: 3rem;
        background: -webkit-linear-gradient(45deg, #00d2ff, #3a7bd5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0;
    }
    .sub-header {
        text-align: center;
        font-size: 1.2rem;
        color: #8b949e;
        margin-bottom: 2rem;
    }
    .stButton>button {
        background: linear-gradient(90deg, #00d2ff 0%, #3a7bd5 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 15px rgba(0, 210, 255, 0.3);
        color: white;
    }
    /* Specific styling for expanders */
    .streamlit-expanderHeader {
        background-color: var(--panel-bg);
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">⚡ AI Pentest Engine</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Automated OWASP Vulnerability Generation Pipeline</p>', unsafe_allow_html=True)

# Layout
col1, col2 = st.columns([1, 2])

with col1:
    st.markdown("### 1. Configuration")
    pdf_file = st.file_uploader("Upload SRS Document (PDF)", type=["pdf"])
    
    num_critical = st.slider("Critical Requirements to Test", min_value=5, max_value=50, value=25, step=5)
    
    run_btn = st.button("🚀 Launch Pentest Pipeline")

def run_step(step_name, cmd, cwd):
    with st.spinner(f"Running {step_name}..."):
        try:
            # Run the command and capture output
            result = subprocess.run(
                cmd, 
                cwd=cwd, 
                shell=True, 
                check=True, 
                capture_output=True, 
                text=True
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            return False, e.stderr

with col2:
    st.markdown("### 2. Execution Pipeline")
    status_container = st.empty()
    logs_expander = st.expander("Terminal Logs", expanded=False)
    
    if run_btn and pdf_file is not None:
        # Save PDF
        input_dir = os.path.join(os.getcwd(), "input")
        os.makedirs(input_dir, exist_ok=True)
        pdf_path = os.path.join(input_dir, "uploaded_srs.pdf")
        with open(pdf_path, "wb") as f:
            f.write(pdf_file.getbuffer())
            
        status_container.info("PDF Uploaded. Starting Pipeline...")
        
        # Step 1
        st.write("🟢 **Step 1: Document Parsing**")
        s1_ok, s1_out = run_step(
            "Document Parsing",
            "python runner.py ../input/uploaded_srs.pdf --output-dir 01_output --doc-id DOC-UPLOADED-SRS",
            "Document_Parsing"
        )
        logs_expander.code(s1_out)
        if not s1_ok:
            st.error("Failed at Document Parsing")
            st.stop()
            
        # Step 2
        st.write("🟢 **Step 2: Requirement Analysis**")
        s2_ok, s2_out = run_step(
            "Requirement Analysis",
            "python runner.py --blocks ../Document_Parsing/01_output/DOC-UPLOADED-SRS_blocks.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/requirements.json",
            "Requirement_Analysis"
        )
        logs_expander.code(s2_out)
        if not s2_ok:
            st.error("Failed at Requirement Analysis")
            st.stop()
            
        # Step 3
        st.write("🟢 **Step 3: Requirement Units Structuring**")
        s3_ok, s3_out = run_step(
            "Requirement Units Structuring",
            "python runner.py --requirements ../Requirement_Analysis/output/requirements.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/cru_units.json",
            "Requirement_Units_Structuring"
        )
        logs_expander.code(s3_out)
        if not s3_ok:
            st.error("Failed at Requirement Units Structuring")
            st.stop()
            
        # Step 4
        st.write("🟢 **Step 4: Segmentation & Classification**")
        s4_ok, s4_out = run_step(
            "Segmentation & Classification",
            "python chunk_domain.py --input ../Requirement_Units_Structuring/output/cru_units.json --output output/chunked_crus.json",
            "Segmentation_and_Classification"
        )
        logs_expander.code(s4_out)
        if not s4_ok:
            st.error("Failed at Segmentation")
            st.stop()
            
        # Step 5
        st.write("🟢 **Step 5: Testcase Generation** (This may take several minutes)")
        os.environ["NUM_CRITICAL_REQUIREMENTS"] = str(num_critical)
        s5_ok, s5_out = run_step(
            "Testcase Generation",
            "python llm_test_case_gen.py",
            "Testcase_Generation"
        )
        logs_expander.code(s5_out)
        if not s5_ok:
            st.error("Failed at Testcase Generation")
            st.stop()
            
        st.success("🎉 Pipeline Completed Successfully!")
        
        # Load output
        st.markdown("### 3. Results")
        output_dir = "Testcase_Generation/output"
        json_files = glob.glob(os.path.join(output_dir, "*.json"))
        if json_files:
            latest_file = max(json_files, key=os.path.getctime)
            try:
                with open(latest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                st.metric("Total Test Cases Generated", data['metadata']['total_test_cases'])
                
                st.markdown("#### 📥 Download Reports")
                dl_col1, dl_col2, dl_col3 = st.columns(3)
                
                timestamp = latest_file.split('_')[-2] + '_' + latest_file.split('_')[-1].split('.')[0]
                xlsx_files = glob.glob(os.path.join(output_dir, f"*{timestamp}.xlsx"))
                txt_files = glob.glob(os.path.join(output_dir, f"*{timestamp}.txt"))
                
                with dl_col1:
                    with open(latest_file, 'rb') as f:
                        st.download_button("Download JSON", data=f, file_name=os.path.basename(latest_file), mime="application/json")
                        
                with dl_col2:
                    if xlsx_files:
                        with open(xlsx_files[0], 'rb') as f:
                            st.download_button("Download Excel (.xlsx)", data=f, file_name=os.path.basename(xlsx_files[0]), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                            
                with dl_col3:
                    if txt_files:
                        with open(txt_files[0], 'rb') as f:
                            st.download_button("Download Summary (.txt)", data=f, file_name=os.path.basename(txt_files[0]), mime="text/plain")
                
                st.markdown("---")
                
                p1 = data.get('phase1_test_cases', [])
                p2 = data.get('phase2_test_cases', [])
                
                tab1, tab2 = st.tabs(["Fast Batch (Phase 1)", "Deep Scans (Phase 2)"])
                
                with tab1:
                    st.dataframe(p1, use_container_width=True)
                with tab2:
                    st.dataframe(p2, use_container_width=True)
                    
            except Exception as e:
                st.error(f"Error loading results: {e}")
                
    elif run_btn and pdf_file is None:
        st.warning("Please upload a PDF first!")
