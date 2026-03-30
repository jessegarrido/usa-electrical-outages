# USA Electrical Outages
This project explores 2022-2023 USA electrical outage data. It demonstrates data cleaning, exploratory analysis, and visualization in Python.

## How to Use
1. Clone this repository from a Git Bash shell    
      git clone https://github.com/jessegarrido/usa-electrical-outages.git
2. Navigate into the project folder
      cd usa-electrical-outages
3. Create a virtual environment
      python3 -m venv outages_venv
4. Activate the virtual environment 
      source outages_venv/Scripts/activate
4. Install dependencies
      pip3 install -r requirements.txt
5. Launch the project in a local browser window 
      marimo edit outages.py  
6. Press Ctr-Shift-R in the browser window to Run All Cells  

## Example Output
The analysis shows that most outages occured in certain states, and most outages arise from weather events. It also shows a linear relationship between outage durations and peak customers impacted. 

## Data Sources
- eaglei_outages_with_events_2023 (provided in `data/`)
- Pacific Northwest Labratory, Department of Energy

## Author
Jesse Garrido – Student Data Analyst

## License
MIT License