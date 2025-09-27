# 🎶 Spotify Artist Analytics  

## 🚀 Project Overview  
This project delivers a **real-time and historical analytics pipeline** for Spotify streaming data.  
It integrates **Apache Airflow, dbt, Snowflake, and Tableau** (all containerized with Docker) to efficiently capture, transform, and visualize Spotify data.  

The system ingests both **historical playlist datasets** and **real-time Spotify Web API streams**, building structured models that power interactive **artist dashboards**. These dashboards highlight **trending tracks, artist popularity, and album performance** with predictive insights for the music industry.  

---

## 💼 Business Problem / Use Case  
Music streaming platforms and record labels face challenges in analyzing **artist performance at scale**:  

- ⚡ Rapidly changing **trending tracks & new releases**  
- 🎤 Lack of **artist-level KPIs** for strategic planning  
- 🔍 Difficulty connecting **metadata (lyrics, genre, style)** to audience behavior  
- 📊 Inefficient manual processes for monitoring top tracks and market trends  

**Our Solution**:  
✅ Automated **real-time + historical pipeline** for Spotify data  
✅ **Artist dashboards** tracking trends, KPIs, and top tracks  
✅ **NLP on metadata** to power AI-driven **recommendations & segmentation**  
✅ Supports **campaign optimization** and **audience targeting** for marketing teams  

**Example Use Case**:  
- A label monitors **Taylor Swift’s new releases** in real time.  
- Dashboards show **top tracks by popularity, explicit vs clean breakdown, energy vs danceability**.  
- NLP-based segmentation groups fans by **listening behavior + content type**.  
- Marketing campaigns auto-adjust budgets using a **plan → act → observe loop**.  

---

## 🔗 System Architecture  

**Workflow**:  
1. **Spotify Web API + Historical Dataset** → Raw ingestion via Airflow DAGs  
2. **Snowflake Warehouse** → Secure, scalable SQL storage  
3. **dbt Models** → Transform into analysis-ready tables 
4. **Tableau Dashboards** → Artist insights, trending tracks, audience segmentation  

---

## 📊 Key Features  

- **Real-time Ingestion** → Spotify new releases, trending tracks, and top albums  
- **ETL with Airflow** → Historical + API data pipelines with error handling & idempotency  
- **ELT with dbt** → Abstract models for unified track/artist/album analysis  
- **Snowflake Warehouse** → Secure storage & SQL-based analytics  
- **Tableau Dashboards** →  
  - Top tracks by popularity  
  - Track distribution by source (historical vs new vs trending)  
  - Artist KPIs (popularity, energy, valence, danceability)  
  - Explicit vs non-explicit content  
  - Audio feature comparisons across albums  
- **NLP & AI Layer** → Metadata-driven recommendations, BLEU/ROUGE/perplexity for validation  
- **Campaign Optimization** → Marketing pipeline with feedback loop for budget allocation  

---

## 🛠️ Languages & Tools  

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/SQL-336791?logo=postgresql&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/Tableau-E97627?logo=tableau&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/NLP-CC0000?logo=ai&logoColor=white&style=for-the-badge" />
  <img src="https://img.shields.io/badge/LLM%20Evaluation-4CAF50?logo=openai&logoColor=white&style=for-the-badge" />
</p>  

---

## 📈 Example Visualizations  

- 🎵 **Top Tracks by Popularity** – bar chart by artist  
- 🥧 **Track Distribution** – historical vs new vs trending  
- 📊 **Artist KPI Cards** – popularity, energy, valence, danceability  
- 🔥 **Danceability vs Energy** – evolution across albums  
- 🧾 **Explicit vs Non-Explicit** – audience segmentation  
- ⏱️ **Track Duration Analysis** – avg playtime per track  

---

## 📂 Repository  

📌 [DW_Spotify_Analytics](https://github.com/28mamtajha/DW_Spotify_Analytics)  

---

