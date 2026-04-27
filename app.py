import streamlit as st
import os
import subprocess
import glob
import json
import time
import signal
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(page_title="AI Pentest Engine", layout="wide", page_icon="")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@400;500;600&family=Syne:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {
        --maroon:       #6B1A1A;
        --maroon-deep:  #4a1010;
        --maroon-muted: #f7f0f0;
        --maroon-mid:   #c9a0a0;
        --black:        #111010;
        --gray-dark:    #3a3838;
        --gray-mid:     #7a7575;
        --gray-light:   #d4cece;
        --gray-faint:   #f8f6f6;
        --white:        #ffffff;
        --border:       #e8e2e2;
    }

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
    }

    .stApp {
        background-color: var(--white);
        color: var(--black);
    }

    /* -- Hide Streamlit chrome -- */
    #MainMenu, footer, header { visibility: hidden; }

    /* -- Decorative top bar -- */
    .stApp::before {
        content: '';
        display: block;
        height: 3px;
        background: linear-gradient(90deg, var(--maroon-deep) 0%, var(--maroon) 40%, var(--maroon-mid) 100%);
        position: fixed;
        top: 0; left: 0; right: 0;
        z-index: 9999;
    }

    /* -- Center the entire content -- */
    .block-container {
        max-width: 1100px !important;
        margin: 0 auto !important;
        padding: 3.5rem 2rem 4rem !important;
    }

    /* -- Header -- */
    .app-header {
        text-align: center;
        margin-bottom: 0.5rem;
        padding-bottom: 2.5rem;
        border-bottom: 1px solid var(--border);
        position: relative;
    }

    .app-eyebrow {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.68rem;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        color: var(--maroon);
        margin-bottom: 0.75rem;
    }

    .app-title {
        font-family: 'Cormorant Garamond', serif;
        font-weight: 600;
        font-size: 3.6rem;
        letter-spacing: -0.01em;
        line-height: 1;
        color: var(--black);
        margin: 0;
    }

    .app-title span {
        color: var(--maroon);
    }

    .app-subtitle {
        font-family: 'Syne', sans-serif;
        font-size: 0.78rem;
        font-weight: 400;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--gray-mid);
        margin-top: 0.9rem;
    }

    .app-header::after {
        content: '';
        display: block;
        width: 40px;
        height: 2px;
        background: var(--maroon);
        margin: 1.5rem auto 0;
    }

    /* -- Section labels -- */
    h3 {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.18em !important;
        text-transform: uppercase !important;
        color: var(--maroon) !important;
        margin-bottom: 1.25rem !important;
        margin-top: 0 !important;
    }

    h4 {
        font-family: 'Syne', sans-serif !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.08em !important;
        color: var(--gray-dark) !important;
        text-transform: uppercase !important;
    }

    /* -- File uploader -- */
    [data-testid="stFileUploader"] {
        border: 1px dashed var(--maroon-mid) !important;
        border-radius: 2px !important;
        background: var(--maroon-muted) !important;
        padding: 0.25rem !important;
        transition: border-color 0.2s !important;
    }

    [data-testid="stFileUploader"]:hover {
        border-color: var(--maroon) !important;
    }

    /* Label above the uploader */
    [data-testid="stFileUploader"] label,
    [data-testid="stFileUploader"] label p {
        font-size: 0.8rem !important;
        color: var(--gray-dark) !important;
    }

    /* "Upload test" text and secondary copy inside the drop zone */
    [data-testid="stFileUploaderDropzone"] span,
    [data-testid="stFileUploaderDropzone"] p,
    [data-testid="stFileUploaderDropzone"] small,
    [data-testid="stFileUploaderDropzone"] button,
    [data-testid="stFileUploaderDropzone"] {
        color: var(--gray-dark) !important;
        background-color: transparent !important;
    }

    /* "Browse files" button inside uploader */
    [data-testid="stFileUploaderDropzone"] button {
        background-color: var(--white) !important;
        color: var(--maroon) !important;
        border: 1px solid var(--maroon-mid) !important;
        border-radius: 2px !important;
        font-family: 'Syne', sans-serif !important;
        font-size: 0.75rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.06em !important;
        padding: 0.35rem 0.9rem !important;
        width: auto !important;
        transition: background-color 0.2s, border-color 0.2s !important;
    }

    [data-testid="stFileUploaderDropzone"] button:hover {
        background-color: var(--maroon-muted) !important;
        border-color: var(--maroon) !important;
        color: var(--maroon-deep) !important;
    }

    /* Upload icon SVG */
    [data-testid="stFileUploaderDropzone"] svg {
        fill: var(--maroon-mid) !important;
        color: var(--maroon-mid) !important;
    }

    /* -- Slider -- */
    [data-testid="stSlider"] > div > div > div > div {
        background: var(--maroon) !important;
    }

    /* -- Text input -- */
    .stTextInput > div > div > input {
        border: 1px solid var(--border) !important;
        border-radius: 2px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.8rem !important;
        color: var(--black) !important;
        background-color: var(--gray-faint) !important;
        padding: 0.55rem 0.85rem !important;
        transition: border-color 0.2s, box-shadow 0.2s !important;
    }

    .stTextInput > div > div > input:focus {
        border-color: var(--maroon) !important;
        box-shadow: 0 0 0 3px rgba(107, 26, 26, 0.07) !important;
        background: var(--white) !important;
    }

    /* -- Expander -- */
    .streamlit-expanderHeader,
    .streamlit-expanderHeader:hover,
    .streamlit-expanderHeader:focus,
    .streamlit-expanderHeader:active,
    .streamlit-expanderHeader[aria-expanded="true"],
    .streamlit-expanderHeader[aria-expanded="false"],
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary:hover,
    [data-testid="stExpander"] summary:focus,
    [data-testid="stExpander"] summary:active {
        background-color: var(--gray-faint) !important;
        border: 1px solid var(--border) !important;
        border-radius: 2px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.08em !important;
        color: var(--gray-dark) !important;
        box-shadow: none !important;
    }

    /* Expander inner p/span text must stay dark too */
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] summary span {
        color: var(--gray-dark) !important;
    }

    .streamlit-expanderContent,
    [data-testid="stExpander"] > div:last-child {
        border: 1px solid var(--border) !important;
        border-top: none !important;
        background: var(--white) !important;
    }

    /* -- Primary button -- */
    .stButton > button {
        background-color: var(--maroon) !important;
        color: var(--white) !important;
        border: none !important;
        border-radius: 2px !important;
        padding: 0.7rem 1.5rem !important;
        font-family: 'Syne', sans-serif !important;
        font-size: 0.78rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.1em !important;
        text-transform: uppercase !important;
        width: 100% !important;
        transition: background-color 0.2s, box-shadow 0.2s, transform 0.15s !important;
        position: relative !important;
        overflow: hidden !important;
    }

    .stButton > button::after {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(135deg, rgba(255,255,255,0.08) 0%, transparent 60%);
        pointer-events: none;
    }

    .stButton > button:hover {
        background-color: var(--maroon-deep) !important;
        box-shadow: 0 4px 20px rgba(107, 26, 26, 0.28) !important;
        transform: translateY(-1px) !important;
    }

    .stButton > button:active {
        transform: translateY(0) !important;
    }

    /* -- Download buttons -- */
    .stDownloadButton > button {
        background-color: transparent !important;
        color: var(--maroon) !important;
        border: 1px solid var(--maroon-mid) !important;
        border-radius: 2px !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.72rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.06em !important;
        padding: 0.55rem 1rem !important;
        width: 100% !important;
        transition: border-color 0.2s, background 0.2s, color 0.2s !important;
    }

    .stDownloadButton > button:hover {
        background-color: var(--maroon-muted) !important;
        border-color: var(--maroon) !important;
        color: var(--maroon-deep) !important;
    }

    /* -- Code / logs -- */
    .stCode, code, pre {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.75rem !important;
        background-color: #faf8f8 !important;
        border: 1px solid var(--border) !important;
        border-radius: 2px !important;
        color: var(--gray-dark) !important;
        line-height: 1.65 !important;
    }

    /* -- Alert banners -- */
    .stAlert {
        border-radius: 2px !important;
        border-left-width: 3px !important;
        font-size: 0.82rem !important;
        font-family: 'Syne', sans-serif !important;
    }

    /* -- Metric -- */
    [data-testid="stMetricValue"] {
        font-family: 'Cormorant Garamond', serif !important;
        font-size: 3rem !important;
        font-weight: 600 !important;
        color: var(--maroon) !important;
        line-height: 1 !important;
    }

    [data-testid="stMetricLabel"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.65rem !important;
        letter-spacing: 0.14em !important;
        text-transform: uppercase !important;
        color: var(--gray-mid) !important;
    }

    /* -- Tabs -- */
    .stTabs [data-baseweb="tab-list"] {
        border-bottom: 1px solid var(--border) !important;
        gap: 0 !important;
        background: transparent !important;
    }

    .stTabs [data-baseweb="tab"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.7rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.1em !important;
        text-transform: uppercase !important;
        color: var(--gray-mid) !important;
        padding: 0.6rem 1.4rem !important;
        border-bottom: 2px solid transparent !important;
        background: transparent !important;
        transition: color 0.2s !important;
    }

    .stTabs [aria-selected="true"] {
        color: var(--maroon) !important;
        border-bottom-color: var(--maroon) !important;
        background-color: transparent !important;
    }

    /* -- Dataframe -- */
    .dataframe thead th {
        background-color: var(--gray-faint) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        letter-spacing: 0.08em !important;
        text-transform: uppercase !important;
        color: var(--gray-mid) !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* -- Caption / small text -- */
    .stCaption, small {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.68rem !important;
        color: var(--gray-mid) !important;
    }

    /* -- st.write / markdown text -- */
    .stMarkdown p {
        font-size: 0.84rem;
        color: var(--gray-dark);
        line-height: 1.6;
    }

    /* -- Column divider -- */
    [data-testid="column"]:first-child {
        border-right: 1px solid var(--border);
        padding-right: 2.5rem !important;
    }

    [data-testid="column"]:last-child {
        padding-left: 2.5rem !important;
    }

    /* -- Step row indicators -- */
    .step-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        padding: 0.55rem 0;
        border-bottom: 1px solid var(--border);
        font-family: 'Syne', sans-serif;
        font-size: 0.8rem;
        color: var(--gray-dark);
    }

    .step-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        background: var(--maroon);
        flex-shrink: 0;
    }
</style>
""", unsafe_allow_html=True)

# -- Header --
st.markdown("""
<div class="app-header">
    <div class="app-eyebrow">Security Intelligence Platform</div>
    <div class="app-title">AI Pentest <span>Engine</span></div>
    <div class="app-subtitle">Automated OWASP Vulnerability Generation Pipeline</div>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col1, col2 = st.columns([1, 2])

with col1:
    st.markdown("### Configuration")
    pdf_file = st.file_uploader("Upload SRS Document (PDF)", type=["pdf"])

    st.markdown("<br>", unsafe_allow_html=True)
    num_critical = st.slider("Critical Requirements to Test", min_value=5, max_value=50, value=25, step=5)

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("Advanced Settings", expanded=False):
        model_name = st.text_input("Ollama Model Name", value="llama3")
        st.caption("Common models: llama3, llama3:8b, mistral, qwen2.5:14b-instruct")

    st.markdown("<br>", unsafe_allow_html=True)
    run_btn = st.button("Launch Pentest Pipeline")

# Status Persistence
STATUS_FILE = os.path.join(BASE_DIR, ".pipeline_status.json")

def load_status():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                return json.load(f)
        except:
            return None
    return None

def save_status(stage, logs="", is_running=False, pid=None):
    status = {
        "stage": stage,
        "is_running": is_running,
        "pid": pid,
        "last_update": time.time()
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f)

def clear_status():
    if os.path.exists(STATUS_FILE):
        try:
            os.remove(STATUS_FILE)
        except:
            pass

def is_pid_running(pid):
    if pid is None: return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def run_step(step_name, cmd, cwd, logs_container=None):
    abs_cwd = os.path.join(BASE_DIR, cwd)
    if logs_container is None:
        logs_container = st.empty()

    log_path = os.path.join(BASE_DIR, "pipeline.log")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"\n\n>>> STARTING STEP: {step_name} at {time.ctime()} <<<\n")
        log_file.flush()

        process = subprocess.Popen(
            cmd,
            cwd=abs_cwd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
            universal_newlines=True
        )

        save_status(step_name, is_running=True, pid=process.pid)

        full_output = []
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                full_output.append(line)
                log_file.write(line)
                log_file.flush()
                logs_container.code("".join(full_output[-15:]))

        rc = process.poll()
        if rc == 0:
            save_status(step_name, is_running=False)
            return True, "".join(full_output)
        else:
            save_status(step_name, is_running=False)
            return False, "".join(full_output)

def is_ollama_online():
    import requests
    try:
        r = requests.get("http://localhost:11434/api/tags")
        return r.status_code == 200
    except:
        return False

with col2:
    st.markdown("### Execution Pipeline")
    status_container = st.empty()

    pipeline_status = load_status()
    is_already_running = False
    if pipeline_status and pipeline_status.get("is_running"):
        if is_pid_running(pipeline_status.get("pid")):
            is_already_running = True
            st.info(f"Pipeline is currently running: {pipeline_status.get('stage')}")
            if st.button("Stop Pipeline"):
                try:
                    os.kill(pipeline_status.get("pid"), signal.SIGTERM)
                    st.success("Sent termination signal. Refreshing...")
                    clear_status()
                    time.sleep(1)
                    st.rerun()
                except:
                    st.error("Failed to stop process.")

    logs_expander = st.expander("Terminal Logs", expanded=is_already_running)
    logs_display = logs_expander.empty()

    if is_already_running:
        log_path = os.path.join(BASE_DIR, "pipeline.log")
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                content = f.readlines()
                logs_display.code("".join(content[-50:]))
        time.sleep(2)
        st.rerun()

    if run_btn and pdf_file is not None and not is_already_running:
        log_path = os.path.join(BASE_DIR, "pipeline.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"--- Pipeline Initialized at {time.ctime()} ---\n")

        input_dir = os.path.join(BASE_DIR, "input")
        os.makedirs(input_dir, exist_ok=True)
        pdf_path = os.path.join(input_dir, "uploaded_srs.pdf")
        with open(pdf_path, "wb") as f:
            f.write(pdf_file.getbuffer())

        status_container.info("PDF uploaded. Starting pipeline...")

        st.markdown('<div class="step-row"><div class="step-dot"></div>Step 1 - Document Parsing</div>', unsafe_allow_html=True)
        s1_ok, s1_out = run_step(
            "Document Parsing",
            "python runner.py ../input/uploaded_srs.pdf --output-dir 01_output --doc-id DOC-UPLOADED-SRS",
            "Document_Parsing",
            logs_display
        )
        if not s1_ok:
            st.error("Failed at Document Parsing")
            st.stop()

        st.markdown('<div class="step-row"><div class="step-dot"></div>Step 2 - Requirement Analysis</div>', unsafe_allow_html=True)

        if not is_ollama_online():
            st.error("Ollama is not running. Please start the Ollama server first.")
            st.stop()

        os.makedirs(os.path.join(BASE_DIR, "Requirement_Analysis", "output"), exist_ok=True)
        s2_ok, s2_out = run_step(
            "Requirement Analysis",
            f"python runner.py --blocks ../Document_Parsing/01_output/DOC-UPLOADED-SRS_blocks.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/requirements.json --model {model_name} --timeout 600",
            "Requirement_Analysis",
            logs_display
        )
        if not s2_ok:
            st.error("Failed at Requirement Analysis")
            st.stop()

        st.markdown('<div class="step-row"><div class="step-dot"></div>Step 3 - Requirement Units Structuring</div>', unsafe_allow_html=True)
        os.makedirs(os.path.join(BASE_DIR, "Requirement_Units_Structuring", "output"), exist_ok=True)
        s3_ok, s3_out = run_step(
            "Requirement Units Structuring",
            "python runner.py --requirements ../Requirement_Analysis/output/requirements.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/cru_units.json",
            "Requirement_Units_Structuring",
            logs_display
        )
        if not s3_ok:
            st.error("Failed at Requirement Units Structuring")
            st.stop()

        st.markdown('<div class="step-row"><div class="step-dot"></div>Step 4 - Segmentation & Classification</div>', unsafe_allow_html=True)
        s4_ok, s4_out = run_step(
            "Segmentation & Classification",
            "python chunk_domain.py --input ../Requirement_Units_Structuring/output/cru_units.json --output output/chunked_crus.json",
            "Segmentation_and_Classification",
            logs_display
        )
        if not s4_ok:
            st.error("Failed at Segmentation")
            st.stop()

        st.markdown('<div class="step-row"><div class="step-dot"></div>Step 5 - Testcase Generation (this may take several minutes)</div>', unsafe_allow_html=True)
        os.environ["NUM_CRITICAL_REQUIREMENTS"] = str(num_critical)
        os.environ["OLLAMA_MODEL"] = model_name
        s5_ok, s5_out = run_step(
            "Testcase Generation",
            "python llm_test_case_gen.py",
            "Testcase_Generation",
            logs_display
        )
        if not s5_ok:
            st.error("Failed at Testcase Generation")
            st.stop()

        st.success("Pipeline completed successfully.")
        clear_status()

        st.markdown("### Results")
        output_dir = os.path.join(BASE_DIR, "Testcase_Generation", "output")
        json_files = glob.glob(os.path.join(output_dir, "*.json"))
        if json_files:
            latest_file = max(json_files, key=os.path.getctime)
            try:
                with open(latest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                st.metric("Total Test Cases Generated", data['metadata']['total_test_cases'])

                st.markdown("#### Download Reports")
                dl_col1, dl_col2, dl_col3 = st.columns(3)

                timestamp_parts = latest_file.split('_')
                timestamp = timestamp_parts[-2] + '_' + timestamp_parts[-1].split('.')[0]
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

                tab1, tab2 = st.tabs(["Fast Batch - Phase 1", "Deep Scans - Phase 2"])

                with tab1:
                    st.dataframe(p1, use_container_width=True)
                with tab2:
                    st.dataframe(p2, use_container_width=True)

            except Exception as e:
                st.error(f"Error loading results: {e}")

    elif run_btn and pdf_file is None:
        st.warning("Please upload a PDF file to proceed.")