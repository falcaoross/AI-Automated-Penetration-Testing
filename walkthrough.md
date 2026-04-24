# ⚡ AI Pentest Engine — Google Colab Run Guide

Complete, step-by-step guide to running the full AI Penetration Testing pipeline **with the Streamlit UI** on Google Colab.

---

## Prerequisites

- A Google account with access to [Google Colab](https://colab.research.google.com)
- Your project repository uploaded to **Google Drive** (or cloned from GitHub in Colab)
- Your `mobile-banking-SRS.pdf` (or any other SRS PDF) ready to upload

---

## Step 0 — Mount Google Drive & Clone the Repo

Run this in the **first cell**:

```python
from google.colab import drive
drive.mount('/content/drive')

# Clone or navigate to your project
# Option A: if the project is on GitHub
!git clone https://github.com/YOUR_USERNAME/Autopilot-QA.git /content/Autopilot-QA

# Option B: if the project is already on your Google Drive
import shutil
shutil.copytree('/content/drive/MyDrive/Autopilot-QA', '/content/Autopilot-QA')
```

Navigate into the project root:
```python
%cd /content/Autopilot-QA
```

---

## Step 1 — Install All Dependencies

```python
!pip install -r requirements.txt -q
!pip install streamlit -q
```

---

## Step 2 — Install and Start Ollama

Colab does not have Ollama pre-installed. Run this to set it up:

```bash
# Install Ollama
!curl -fsSL https://ollama.com/install.sh | sh

# Start the Ollama server in the background
!nohup ollama serve &>/content/ollama.log &

# Wait a few seconds for the server to start
import time
time.sleep(5)

# Pull the LLM model
!ollama pull llama3:8b
```

> **Note:** The model download (~4.7 GB) will take a few minutes. Wait for it to fully complete before moving on.

---

## Step 3 — Launch the Streamlit UI

Run the following in a **single cell**. This will start the app AND give you a public link to access it:

```python
# Install localtunnel (needed to expose the UI from Colab to your browser)
!npm install localtunnel -q

# Start Streamlit in the background
!streamlit run app.py &>/content/streamlit.log &

# Wait for it to start
import time
time.sleep(4)

# Print your public endpoint IP (you will need this as the password on the localtunnel page)
import urllib
ip = urllib.request.urlopen('https://ipv4.icanhazip.com').read().decode('utf8').strip()
print(f"🔑 Your endpoint password is: {ip}")
print("Copy this IP address — you will need it in the next step!\n")

# Start localtunnel to get a public URL for port 8501
!npx localtunnel --port 8501
```

---

## Step 4 — Access the UI in Your Browser

1. Wait for `localtunnel` to print a line like:
   ```
   your url is: https://xxxx-xxxx.loca.lt
   ```
2. Click that URL.
3. It will show an **"Endpoint IP"** prompt — paste the IP address that was printed in the cell above.
4. Click **"Click to Submit"**.
5. ✅ You are now inside the AI Pentest Engine UI!

---

## Step 5 — Run the Pipeline from the UI

Once the UI loads in your browser:

1. **Upload your SRS PDF** — drag and drop `mobile-banking-SRS.pdf` into the uploader on the left panel.
2. **Set the number of Critical Requirements** using the slider (default: 25).
3. Click **🚀 Launch Pentest Pipeline**.
4. Watch the pipeline run through all 5 stages automatically:
   - 🟢 Document Parsing
   - 🟢 Requirement Analysis
   - 🟢 Requirement Units Structuring
   - 🟢 Segmentation & Classification
   - 🟢 Testcase Generation *(this is the longest step — typically 5–20 mins depending on the model)*
5. When done, the UI will display a table of all generated OWASP test cases.

---

## Step 6 — Download Your Results

Once generation completes, three download buttons will appear in the UI:

| Button | File | Description |
|---|---|---|
| **Download JSON** | `optimized_test_cases_TIMESTAMP.json` | Full structured data — all phases |
| **Download Excel (.xlsx)** | `optimized_test_cases_TIMESTAMP.xlsx` | Multi-sheet workbook with all test cases |
| **Download Summary (.txt)** | `optimized_test_cases_summary_TIMESTAMP.txt` | Human-readable summary of what was generated |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Streamlit link not working | Wait 10 seconds and refresh the cell, then re-run `!npx localtunnel --port 8501` |
| `ollama: command not found` | Re-run Step 2 — the Colab runtime may have reset |
| Step 5 (Testcase Generation) fails | Check that Ollama is running: `!pgrep -x ollama` — if no output, re-run `!nohup ollama serve &` |
| Model not found error | Run `!ollama pull llama3:8b` again and wait for full completion |
| Terminal logs window is empty | Expand the **"Terminal Logs"** dropdown inside the UI to see live output |
