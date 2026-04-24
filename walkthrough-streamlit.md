# Welcome to the AI Pentest Engine UI ⚡

I've successfully created a completely autonomous, unified frontend for your Pentest Generation Pipeline! 

The new UI is built with **Streamlit** and features a very clean, dark-mode styling with neon accents (glassmorphism/premium aesthetic). It wraps around your existing Python modules, meaning it won't break any of your hard work!

### How it Works:
1. You just drag and drop your `SRS.pdf` into the browser.
2. Select how many critical requirements you want to deep-scan.
3. Click the giant **Launch Pentest Pipeline** button.
4. The UI will automatically run through **Steps 1 to 5** in the background, showing you a live terminal log inside a dropdown.
5. When finished, it will automatically parse the output JSON and display a beautiful, sortable table of all your OWASP test cases directly on the webpage!

---

## 🏃 How to Run the UI

### Method 1: Running Locally (Windows/Mac)
If you are running this on your own machine, simply open a terminal in your `Autopilot-QA` folder and type:
```bash
pip install -r requirements.txt
streamlit run app.py
```
*Your browser will automatically open to `http://localhost:8501`.*

### Method 2: Running in Google Colab
Since Colab runs on a cloud server, you can't just open `localhost`. Instead, we use a tool called `localtunnel` to give you a public web link to your Streamlit app!

Run this in a single Colab cell:
```bash
!pip install streamlit -q
!npm install localtunnel

# Start streamlit in the background
!streamlit run app.py &>/content/logs.txt &

# Get your public IP address (you will need this as the password for localtunnel)
import urllib
print("Password/Enpoint IP for localtunnel is:", urllib.request.urlopen('https://ipv4.icanhazip.com').read().decode('utf8').strip("\n"))

# Start localtunnel on port 8501
!npx localtunnel --port 8501
```
Click the link that `localtunnel` prints out, paste the IP address it gave you as the password, and you're in!
