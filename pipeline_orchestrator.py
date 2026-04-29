import os
import subprocess
import time
import sys
import argparse
from pipeline_utils import save_status, LOG_PATH, BASE_DIR

def run_background_step(step_name, cmd, cwd):
    abs_cwd = os.path.join(BASE_DIR, cwd)
    
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(f"\n\n>>> STARTING STEP: {step_name} at {time.ctime()} <<<\n")
        log_file.flush()

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

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

        save_status(step_name, is_running=True, pid=os.getpid())

        for line in process.stdout:
            log_file.write(line)
            log_file.flush()

        process.wait()
        return process.returncode == 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="llama3")
    parser.add_argument("--num_critical", type=int, default=25)
    args = parser.parse_args()

    # Step 1: Document Parsing
    if not run_background_step(
        "Document Parsing",
        "python runner.py ../input/uploaded_srs.pdf --output-dir 01_output --doc-id DOC-UPLOADED-SRS",
        "Document_Parsing"
    ):
        save_status("Failed at Document Parsing", is_running=False, pid=os.getpid())
        sys.exit(1)

    # Step 2: Requirement Analysis
    os.makedirs(os.path.join(BASE_DIR, "Requirement_Analysis", "output"), exist_ok=True)
    if not run_background_step(
        "Requirement Analysis",
        f"python runner.py --blocks ../Document_Parsing/01_output/DOC-UPLOADED-SRS_blocks.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/requirements.json --model {args.model} --timeout 600",
        "Requirement_Analysis"
    ):
        save_status("Failed at Requirement Analysis", is_running=False, pid=os.getpid())
        sys.exit(1)

    # Step 3: Requirement Units Structuring
    os.makedirs(os.path.join(BASE_DIR, "Requirement_Units_Structuring", "output"), exist_ok=True)
    if not run_background_step(
        "Requirement Units Structuring",
        "python runner.py --requirements ../Requirement_Analysis/output/requirements.json --skeleton ../Document_Parsing/01_output/DOC-UPLOADED-SRS_skeleton.json --output output/cru_units.json",
        "Requirement_Units_Structuring"
    ):
        save_status("Failed at Requirement Units Structuring", is_running=False, pid=os.getpid())
        sys.exit(1)

    # Step 4: Segmentation & Classification
    if not run_background_step(
        "Segmentation & Classification",
        "python chunk_domain.py --input ../Requirement_Units_Structuring/output/cru_units.json --output output/chunked_crus.json",
        "Segmentation_and_Classification"
    ):
        save_status("Failed at Segmentation", is_running=False, pid=os.getpid())
        sys.exit(1)

    # Step 5: Testcase Generation
    os.environ["NUM_CRITICAL_REQUIREMENTS"] = str(args.num_critical)
    os.environ["OLLAMA_MODEL"] = args.model
    if not run_background_step(
        "Testcase Generation",
        "python llm_test_case_gen.py",
        "Testcase_Generation"
    ):
        save_status("Failed at Testcase Generation", is_running=False, pid=os.getpid())
        sys.exit(1)

    save_status("Completed", is_running=False, pid=os.getpid())
    print("Pipeline Orchestrator finished successfully.")

if __name__ == "__main__":
    main()
