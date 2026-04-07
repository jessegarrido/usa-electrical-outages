# USA Electrical Outages
This project explores 2022-2023 USA electrical outage data. It demonstrates data cleaning, exploratory analysis, and visualization in Python.

## Setup
1. Clone this repository from a Git Bash shell    
      ```git clone https://github.com/jessegarrido/usa-electrical-outages.git```<br>
2. Navigate into the project folder<br>
      ```cd usa-electrical-outages```<br>
3. Create a virtual environment<br>
      ```python3 -m venv outages_venv```<br>
4. Activate the virtual environment <br>
      ```source outages_venv/Scripts/activate```<br>
4. Install dependencies<br>
      ```pip3 install -r requirements.txt```<br>
5. Launch the project in a local browser window <br>
      ```marimo edit outages.py ``` <br>
6. Press Ctr-Shift-R in the browser window to Run All Cells  <br>

## Data Sources (provided in `data/`)
Event-correlated Outage Dataset in America, April 25, 2025
      Pacific Northwest Laboratory, U.S. Department of Energy
      https://catalog.data.gov/dataset/event-correlated-outage-dataset-in-america
      - eaglei_outages_with_events_2022 
      - eaglei_outages_with_events_2023 
GDP and Personal Income Summary by County, 2022-2024
      U.S. Bureau of Economic Analysis 
      https://apps.bea.gov/itable/?ReqID=70&step=1#eyJhcHBpZCI6NzAsInN0ZXBzIjpbMSwyOSwyNSwzMSwyNiwyNywzMF0sImRhdGEiOltbIlRhYmxlSWQiLCI1MzMiXSxbIk1ham9yX0FyZWEiLCI0Il0sWyJTdGF0ZSIsWyJYWCJdXSxbIkFyZWEiLFsiWFgiXV0sWyJTdGF0aXN0aWMiLFsiMSJdXSxbIlVuaXRfb2ZfbWVhc3VyZSIsIkxldmVscyJdLFsiWWVhciIsWyIyMDI0IiwiMjAyMyIsIjIwMjIiXV0sWyJZZWFyQmVnaW4iLCItMSJdLFsiWWVhcl9FbmQiLCItMSJdXX0=
      - CAINC1
      - CAGDP1
Public Assistance Funded Project Summaries - v1
      FEMA, U.S. Department of Homeland Security
      https://www.fema.gov/openfema-data-page/public-assistance-funded-project-summaries-v1
      - PublicAssistanceFundedProjectsSummaries

## Findings
US State and county wealth was observed to weakly correlate with occurence of electrical outages over the period studied. The data highlights that a wealthy economy does not insulate customers from electrical outages, but increases exposure to them. 