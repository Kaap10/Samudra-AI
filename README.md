# Samudra AI - Unified Marine Data Platform Dashboard

A professional, government-style dashboard for marine data, built for Smart India Hackathon.

---

## 🚀 Quick Start (Local Development)

### 1. Backend (FastAPI API)

#### a. Create and activate Python virtual environment
```
python -m venv venv
.\venv\Scripts\activate
```

#### b. Install dependencies
```
pip install fastapi uvicorn[standard] pandas numpy
```

#### c. Clean the raw data (if not already done)
```
python run_processing_pipeline.py
```

#### d. Start the backend API server
```
uvicorn backend_api:app --reload --port 8000
```

- The API will be available at: http://localhost:8000/api/clean_data

### 2. Frontend (React Dashboard)

#### a. Install dependencies
```
cd dashboard-frontend
npm install
```

#### b. Start the React development server
```
npm start
```

- The dashboard will open at: http://localhost:3000
- Make sure the backend is running on port 8000 for data to load.

---

## 🖥️ Features
- Government-inspired UI (india.gov.in style)
- Header with Samudra AI and SIH logos
- KPI cards (auto-calculated from data)
- Interactive data table: sorting, filtering by location, pagination
- Responsive and modern (Material-UI)

---

## 🗃️ Data
- Raw data: `raw_ocean_data.csv`
- Cleaned data: `clean_data_table.csv` (auto-generated)
- Backend serves data from the cleaned CSV for demo purposes

---

## 📝 Customization
- To use a real PostgreSQL DB, update `backend_api.py` to connect to your DB and set credentials (see commented code).
- Replace logo URLs in `dashboard-frontend/src/App.js` with your own.

---

## 🤝 Credits
- Built for Smart India Hackathon
- UI inspired by [india.gov.in](https://www.india.gov.in/)

---
