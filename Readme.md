# NYC Crash and Collision Analysis Project

&#x20;

---

## 📊 Project Overview

This project focuses on analyzing motor vehicle crash and collision data in New York City. It combines crash reports with public holiday information and geographic boundary data to explore trends and generate actionable insights.

The workflow follows a complete data engineering and analytics pipeline: from data extraction and cleaning to merging, transformation, and visualization.

---

## 📆 Data Sources

### 1. Crashes and Collisions Data

- **Specifications:**
  - Date and time of crash
  - Location (Latitude, Longitude, Borough)
  - Vehicle types involved
  - Reason for collision (contributing factors)
  - Count of injuries and fatalities
- **Format:** CSV files
- **Storage:** you can download it from here : (https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes#:~:text=The%20Motor%20Vehicle%20Collisions%20crash,motor%20vehicle%20collisions%20in%20NYC)`

### 2. Public Holidays Data

- **Specifications:**
  - Date of the holiday
  - Holiday name
  - Affected counties
  - Type of holiday (Public/Federal/etc.)
- **Source:** API from [dager.ai](https://publicapis.io/nager-date-api)
- **Format:** JSON files converted into DataFrames

### 3. NYC Borough Boundaries

- Shapefile data for borough boundary mapping.
- Used for geographical visualizations and spatial analyses.
- **Storage:** Located under `assets/NYC_Borough_Boundary_/`

---

## 👷🏻‍💻 Technologies Used

- **Programming Language:** Python 3.10+

- **Libraries:**

  - `pandas` (Data manipulation)
  - `numpy` (Numerical operations)
  - `seaborn` (Data visualization)
  - `matplotlib` (Plotting)
  - `requests` (API calls)
  - `geopy` (Geocoding)
  - `json` (Working with JSON formats)
  - `shapely` (Geospatial operations)
  - `logging` (Logging pipeline events)
  - `os`, `datetime` (System and time utilities)


## 📦 Project Structure

```
NYC_Project/
├── assets/                         # Raw static data (CSV files, Shapefiles, Boundaries)
│   ├── Crashes_Collisions_Dataset/  
│   |   └── Motor_Vehicle_Collisions_Crashes.csv  # Crash datasets
|   |
│   └── NYC_Borough_Boundary_6403672305752144374/
|       └── corrected_boundaries.shx        # NYC borough shapefile
│
├── lib/                            # Python modules (extraction, cleaning, plotting)
│   ├── Modulerized_Crashes.py   
│   └── Modulerized_Holidays.py       
|
├── logs/                           # Log files for debugging and monitoring
|   └── Modulerized_Crashes_Holidays_logs.log
│
├── out/                            # Outputs from the project
│   ├── Charts/                     # Generated plots and charts
|       └── Borough_Collisions.png        
|       └── nyc_collisions_by_public_holiday_by_year.png       
|       └── nyc_collisions_by_public_holiday.png        
|       └── nyc_collisions_by_year.png       
│   └── data/                       # Processed, merged, and cleaned datasets (parquet file)
|   |
│   └── report/ 
|       └── holiday_crashes_profiling_report.html        # html report created by ydata-profiling
│
├── presentation/                  # Presentation materials (slides, PDFs)
|   └── NYC_Collisions_Holidays_Presentation.pptx 
│
├── Full_Pipeline.py               # Master Python script to execute the full pipeline
|
└── README.md                      # Detailed project documentation
```

---

## 🔄 How to Run This Project

1. **Clone the repository**:

   ```bash
   git clone https://github.com/AhmedHShaalan/NYC_Project.git
   cd NYC_Project
   ```

2. **Install dependencies**:

   ```bash
   pip install -r prerequisites.txt
   ```

3. **Run the full pipeline**:

   ```bash
   python Full_Pipeline.py
   ```

4. **View Outputs**:

   - Processed datasets will appear in `out/data/`
   - Visualizations and charts will appear in `out/Charts/`
   - Logs will be saved under `logs/`

---

## 📈 Example Visualizations

- Crash distribution across boroughs
- Impact of holidays on collision rates
- Heatmaps of crash density
- Injury and fatality trends over years

*(Visual samples are saved inside **`out/Charts/`**)*

---


## 💍 Contact

For inquiries, suggestions, or collaborations, feel free to reach out:

- **Name:** Ahmed Shaalan
- **Email:** [ahmed.hesham.shaalan@gmail.com]

---

> "Data is a precious thing and will last longer than the systems themselves." — Tim Berners-Lee

---

✨ Thank you for checking out this project! ✨

