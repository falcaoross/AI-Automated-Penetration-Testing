# AI Pentest Test Case Generation

> **Automated Security Penetration Testing powered by LLMs and OWASP Top 10**



![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)

![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?logo=streamlit)

![Ollama](https://img.shields.io/badge/LLM-Ollama%20llama3:8b-black?logo=ollama)

![OWASP](https://img.shields.io/badge/Standard-OWASP%20Top%2010-red)

![Colab](https://img.shields.io/badge/Runs%20on-Google%20Colab-F9AB00?logo=googlecolab)

![License](https://img.shields.io/badge/License-MIT-green)



---



## Overview



**AI Pentest Engine** is a fully automated, LLM-driven penetration testing pipeline that ingests a Software Requirements Specification (SRS) document and generates comprehensive, actionable security test cases aligned with the **OWASP Top 10** vulnerability framework.



Instead of manually writing pentest scripts, just upload your SRS PDF. The pipeline reads and understands the architecture, extracts security-critical requirements, and generates targeted attack scenarios - covering SQL Injection, XSS, Broken Authentication, SSRF, Insecure Deserialization, and more.



The entire pipeline is wrapped in a beautiful, modern **Streamlit web UI** that runs seamlessly on **Google Colab**.



---



## Key Features



- Multi-format SRS Ingestion - PDF, Word, Markdown, tables

- LLM-based Requirement Extraction - Acts as a Security Requirements Engineer

- OWASP Top 10 Aligned Test Generation - Produces real attack payloads, not generic templates

- Fully Autonomous Pipeline - 5-stage automated execution from raw PDF to test cases

- Modern Streamlit UI - Dark mode, drag-and-drop upload, live progress tracking

- Export Options - Download results as .json, .xlsx, or .txt

- Google Colab Ready - Runs entirely in the cloud with no local setup required



---



## Pipeline Architecture



```

SRS PDF

   │

   ▼

┌-----------------------------┐

│  Stage 1: Document Parsing  │  <- Extracts text blocks and document skeleton

└-----------------------------┘

   │

   ▼

┌----------------------------------┐

│  Stage 2: Requirement Analysis   │  <- LLM extracts security-aware requirements

└----------------------------------┘

   │

   ▼

┌------------------------------------------┐

│  Stage 3: Requirement Units Structuring  │  <- Normalizes to CRU format (security type)

└------------------------------------------┘

   │

   ▼

┌----------------------------------------------┐

│  Stage 4: Segmentation & Classification      │  <- Semantic chunking + domain tagging

└----------------------------------------------┘

   │

   ▼

┌----------------------------------┐

│  Stage 5: Testcase Generation    │  <- LLM generates OWASP attack test cases

└----------------------------------┘

   │

   ▼

 Output: .json + .xlsx + .txt

```



---



## Directory Structure



```

AI-Pentest-Gen/

├-- app.py                          <- Streamlit UI (entry point)

├-- requirements.txt

├-- walkthrough.md                  <- Full Colab run guide

│

├-- Document_Parsing/               <- Stage 1

├-- Requirement_Analysis/           <- Stage 2

├-- Requirement_Units_Structuring/  <- Stage 3

├-- Segmentation_and_Classification/<- Stage 4

├-- Testcase_Generation/            <- Stage 5

│   ├-- llm_test_case_gen.py

│   ├-- prompts.json

│   └-- output/                     <- Generated test cases saved here

│

├-- knowledge_graph/                       <- Advanced: Knowledge Graph RAG module

├-- coverage_validation/                    <- Traceability and Validation layer

└-- input/                          <- Place your SRS PDFs here

```



---



## Tech Stack



| Layer | Tool | Purpose |

|---|---|---|

| **Document Parsing** | PyMuPDF, Camelot | Extract text and tables from SRS PDFs |

| **Requirement Extraction** | Ollama `llama3:8b` | Security-aware requirement understanding |

| **Semantic Chunking** | Sentence Transformers | Chunk and classify requirement units |

| **Test Generation** | Ollama `llama3:8b` | Generate OWASP-aligned pentest scenarios |

| **UI** | Streamlit | Modern dark-mode web interface |

| **Tunneling (Colab)** | localtunnel | Expose Streamlit UI from Colab |



---



## OWASP Vulnerability Coverage



The engine generates test cases targeting the full **OWASP Top 10**:



| # | Category | Examples Generated |

|---|---|---|

| A01 | Broken Access Control | IDOR, privilege escalation, path traversal |

| A02 | Cryptographic Failures | Sensitive data in plaintext, weak TLS |

| A03 | Injection | SQLi, command injection, LDAP injection |

| A04 | Insecure Design | Business logic flaws, abuse cases |

| A05 | Security Misconfiguration | Default credentials, open CORS, verbose errors |

| A06 | Vulnerable Components | Outdated libraries, known CVE exposure |

| A07 | Auth Failures | Brute force, session fixation, weak JWT |

| A08 | Insecure Deserialization | Object injection, RCE via deserialization |

| A09 | Logging Failures | Missing audit logs, unmonitored actions |

| A10 | SSRF | Internal port scanning, metadata endpoint access |



---



## Quick Start



### Running Locally



```bash

git clone https://github.com/YOUR_USERNAME/AI-Pentest-Gen.git

cd AI-Pentest-Gen

pip install -r requirements.txt

streamlit run app.py

```



Open your browser at `http://localhost:8501`.



### Running on Google Colab



See the full step-by-step guide in [`walkthrough.md`](./walkthrough.md).



**TL;DR:**

```python

# In a Colab cell:

!curl -fsSL https://ollama.com/install.sh | sh

!nohup ollama serve &>/content/ollama.log &

!ollama pull llama3:8b

!pip install -r requirements.txt streamlit -q

!npm install localtunnel -q

!streamlit run app.py &>/content/streamlit.log &

import urllib, time; time.sleep(4)

print("Password:", urllib.request.urlopen('https://ipv4.icanhazip.com').read().decode('utf8').strip())

!npx localtunnel --port 8501

```



---



## Output Files



After a successful run, three files are generated in `Testcase_Generation/output/`:



| File | Description |

|---|---|

| `AI_Pentest_Report_OWASP_TIMESTAMP.json` | Full structured output - all phases and metadata |

| `AI_Pentest_Report_OWASP_TIMESTAMP.xlsx` | Multi-sheet workbook with per-type test case tabs |

| `AI_Pentest_Report_OWASP_summary_TIMESTAMP.txt` | Human-readable generation summary |



All three files can be downloaded directly from the UI.



---



## Team



- **Shashank Tiwari** - Team Lead

- Sakshi Kupekar

- Preetham Fernandes

- Aadil Attar



---



## License



This project is licensed under the [MIT License](LICENSE).

