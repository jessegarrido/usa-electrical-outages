import marimo

__generated_with = "0.22.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <h1>US Electrical Power Outages 2022-2023</h1>
    <h2>GDP and Income in Impacted States and Counties</h2>
    """)
    return


@app.cell
def _():
    #Imports
    import marimo as mo
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import datetime
    import sqlite3

    return mo, np, pd, plt, sqlite3


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <h2>Data Importing, Cleaning, and Wrangling</h2>
    Read in DOE outage event data from csv
    """)
    return


@app.cell
def _(pd):
    #Read in DOE outage data
    df = pd.read_csv(r'data\eaglei_outages_with_events_2022.csv')
    df2 = pd.read_csv(r'data\eaglei_outages_with_events_2023.csv')
    df = pd.concat([df,df2])
    df['Datetime Event Began'] = pd.to_datetime(df['Datetime Event Began'], errors='coerce')
    df['Datetime Event Began'] = df['Datetime Event Began'].dt.tz_localize('UTC')
    df['event_id']=df['event_id'] + "-" + df['Datetime Event Began'].dt.year.astype(str)
    df.head()
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Parse outage type from 37 into 4 unique categories
    """)
    return


@app.cell
def _(df, pd):
    # This function evaluates the Event Type column for keywords that indicate a category
    def parse_event(event):
        human_keywords = "attack|suspicious|theft|vandalism|cyber|sabotage"
        system_keywords = "failure|fuel|operations|interruption|inadequacy"
        system_exclude_keywords = "attack|unknown"
        event = pd.Series(event.lower())
        if event.str.contains("weather").any():
            return "Weather"
        if event.str.contains(human_keywords).any():
            return "Human Activity"
        if event.str.contains(system_keywords).any() & ~event.str.contains(system_exclude_keywords).any():
            return "System Failure"
        return "Unknown"
    # The function is called on each row of the dataframe to generate an Event column
    df['Event']=df['Event Type'].apply(parse_event)
    print(df['Event'].value_counts())
    print("\n\nWEATHER:",df[df['Event']=="Weather"]['Event Type'].unique(),
        "\n\nHUMAN ACTIVITY:",df[df['Event']=="Human Activity"]['Event Type'].unique(),
        "\n\nSYSTEM FAILURES:",df[df['Event']=="System Failure"]['Event Type'].unique(),
        "\n\nUNKNOWN:",df[df['Event']=="Unknown"]['Event Type'].unique())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Read in economic data from the US Beareau of Economic Anlysis
    """)
    return


@app.cell
def _(pd):
    #Annual Income Data from Bureau of Economic Analysis
    df3 = pd.read_csv(r'data\CAINC1.csv')
    df3.columns=['fips','name','income2022','income2023','income2024']
    df4 = pd.read_csv(r'data\CAGDP1.csv')
    df4.columns=['fips','name','gdp2022','gdp2023','gdp2024']
    econ_df=pd.merge(df3,df4,on=["fips","name"])

    econ_df = econ_df[econ_df['fips'].notna()]
    econ_df['fips']=pd.to_numeric(econ_df['fips'], errors='coerce')
    econ_df.head()
    return df3, econ_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Reprocess the economic data to meet the database design schema
    """)
    return


@app.cell
def _(econ_df, pd):
    econ_facts_df = pd.DataFrame()
    # Using iterrows()
    for index, row in econ_df.iterrows():
        for year in range(2022,2024):
            new_row = pd.DataFrame({'econ_id': [len(econ_facts_df)], 'fips': [row['fips']],'year': [year],'income':row[f'income{year}'],'gdp':row[f'gdp{year}']})
            econ_facts_df = pd.concat([econ_facts_df,new_row], ignore_index=True)
    econ_facts_df.head()
    return (econ_facts_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create a dataframe for a 'counties' database table from a dictionary containing state abbreviations
    """)
    return


@app.cell
def _(df3):
    #List of states
    state_abbreviations = {
            'AK': 'Alaska',
            'AL': 'Alabama',
            'AR': 'Arkansas',
            'AZ': 'Arizona',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DC': 'District of Columbia',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'IA': 'Iowa',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'MA': 'Massachusetts',
            'MD': 'Maryland',
            'ME': 'Maine',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MO': 'Missouri',
            'MS': 'Mississippi',
            'MT': 'Montana',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'NE': 'Nebraska',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NV': 'Nevada',
            'NY': 'New York',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VA': 'Virginia',
            'VT': 'Vermont',
            'WA': 'Washington',
            'WI': 'Wisconsin',
            'WV': 'West Virginia',
            'WY': 'Wyoming',
            'PR': 'Puerto Rico',
            'VI': 'Virgin Islands'
        }
    counties_df=df3.iloc[:,[0,1]].copy()
    counties_df.loc[:, 'state']=counties_df.name.str.split(',').str[-1]
    counties_df.loc[:, 'name']=counties_df.name.str.split(',').str[0] 

    # Use .loc to explicitly set values on the original DataFrame
    counties_df.loc[:, 'state'] = counties_df['state'].replace('\\*', '', regex=True)
    counties_df.loc[:, 'state'] = (counties_df['state'].astype(str)
            .str.strip() 
            .map(state_abbreviations) 
            .fillna('Unknown')
    )
    counties_df.head()
    return (counties_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Import the table of FEMA grants
    """)
    return


@app.cell
def _(pd):
    #FEMA 
    fema_df = pd.read_csv(r'data\PublicAssistanceFundedProjectsSummaries.csv')
    fema_df=fema_df.iloc[:,[0,1,2,3,5,8]]
    fema_df=fema_df.dropna(subset=['declarationDate'])
    fema_df['Date']=pd.to_datetime(fema_df['declarationDate'])
    fema_df = fema_df[fema_df['Date'].dt.year > 2021]
    fema_df.head()
    return (fema_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Match unique disaster events between outage event and fema grant datasets
    """)
    return


@app.cell
def _(df, fema_df, pd):
    # This function iterates over two data frames, examining a date from each to identify references to the same event    
    def match_events(df1,df2):  
        df1['declarationDate'] = pd.to_datetime(df1['declarationDate'], errors='coerce')
        df1['event_id'] = None
        df1['Datetime Event Began'] = None
        for pos1, (_, row1) in enumerate(df1.iterrows()):
            for pos2, (_, row2) in enumerate(df2.iterrows()):
                diff_days = row1['declarationDate'] - row2['Datetime Event Began']
                if (row2['state_event'] == row1['state']) & (diff_days.days <= 20) & (diff_days.days >= 0) & (row2['Event'] == 'Weather'):
                    df1.iat[pos1,-2] = row2['event_id']
                    df1.iat[pos1,-1] = row2['Datetime Event Began']
        return df1           
    # create unique events db 
    events_df=df.drop_duplicates('event_id')
    events_df=events_df.iloc[:,[0,1,2,14]]
    #create unique fema db
    disasters_df=fema_df.drop_duplicates('disasterNumber')
    disasters_df=disasters_df.iloc[:,[0,3,1,2]]
    # use the function to match unique disasters from the events table to the FEMA grants table 
    disasters_df=match_events(disasters_df,events_df)
    disasters_df.head()
    return (disasters_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create dataframe of normalized and summary fema grant data for database
    """)
    return


@app.cell
def _(disasters_df, fema_df, pd):
    fema_df2 = pd.merge(fema_df,disasters_df,on="disasterNumber", how="left")
    fema_df2=fema_df2.iloc[:,[10,5]]
    fema_df3=fema_df2.groupby('event_id').sum()
    fema_total=fema_df3['federalObligatedAmount'].sum()
    fema_df3['pct_of_fema']=round(fema_df3['federalObligatedAmount']/fema_total*100,2)
    fema_df3.head()
    return (fema_df3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create fema dataframe to database schema
    """)
    return


@app.cell
def _(disasters_df, fema_df3, pd):
    fema_facts_df = pd.merge(fema_df3,disasters_df,on="event_id", how="left")
    fema_facts_df['disaster_id']=fema_facts_df.index+1
    fema_facts_df=fema_facts_df.iloc[:,[8,0,5,6,4,1,2]]
    fema_facts_df.columns=['disaster_id','name','date','type','state','amount','percent']
    fema_facts_df.head()
    return (fema_facts_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create events dataframe to database schema
    """)
    return


@app.cell
def _(df, fema_facts_df, pd):
    events_table_df = pd.merge(df,fema_facts_df,how="left",left_on='event_id',right_on='name')
    events_table_df['outage_id']=events_table_df.index+1
    events_table_df=events_table_df.iloc[:,[-1,15,14,5,6,7,8,10,9,11,12,13]]
    events_table_df.columns=['outage_id','disaster_id','type','fips','state','county','start_time','end_time','duration','min_customers','max_customers','mean_customers']
    events_table_df.head()
    return (events_table_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <h2>Create the project database</h2>
    """)
    return


@app.cell
def _(sqlite3):
    connection = sqlite3.connect("outages.db")
    connection
    return (connection,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Populate the database with wrangled data
    """)
    return


@app.cell
def _(connection, counties_df, econ_facts_df, events_table_df, fema_facts_df):
    #tables = [events_table_df,fema_facts_df,counties_df,econ_facts_df]
    events_table_df.to_sql('outages', con=connection, if_exists='replace', index=False)
    fema_facts_df.to_sql('fema', con=connection, if_exists='replace', index=False)
    counties_df.to_sql('counties', con=connection, if_exists='replace', index=False)
    econ_facts_df.to_sql('econ_facts', con=connection, if_exists='replace', index=False)
    return


@app.cell
def _(connection, pd):
    pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", connection)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Define a function to drop columns in a sqlite3 table by index
    """)
    return


@app.cell
def _(connection):
    #Function to drop a Sqlite3 by index
    def drop_by_index(db_path, table, indices):
        cursor = connection.cursor()

        # Get names from indices
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()

        # Sort indices in reverse to avoid shifting if you were deleting by index in a list, 
        # but since we drop by name, we just need to ensure the index exists.
        names_to_drop = [cols[i][1] for i in indices if i < len(cols)]

        for name in names_to_drop:
            cursor.execute(f'ALTER TABLE "{table}" DROP COLUMN "{name}"')

        connection.commit()

    return (drop_by_index,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Define a function to normalize outage data. Call the function twice to generate normalized dataframes of state and county outage data.
    """)
    return


@app.cell
def _(connection, drop_by_index, pd):
    #Function takes 'state' or 'fips' as an argument to generate normalized outage statistics. Normalization is accomplished by translating summary values into percent of total recorded.    
    def normalize(scope):
        query1 = f""" 
            select c.name as county, o.{scope}, count(*) as number_of_events, sum(duration * mean_customers) as summary_customer_hours, sum(duration) as summary_duration from outages o
            join econ_facts e on strftime('%Y', o.start_time) = e.year AND o.fips=e.fips
            join counties c on c.fips = o.fips
            group by c.{scope}
            order by number_of_events desc
        """
        query2 = f"""
                select  distinct  c.state as state2, c.{scope},sum(gdp/2) over (partition by c.{scope}) as summary_gdp, sum(income/2) over (partition by c.{scope}) as summary_income from econ_facts e
                left join counties c on e.fips = c.fips
                order by income desc
        """
        events = pd.read_sql(query1, connection)
        econ = pd.read_sql(query2, connection)
        events=pd.merge(events,econ,on=[scope])
        total_events = events['number_of_events'].sum()
        total_duration = events['summary_duration'].sum()
        total_customer_hours = events['summary_customer_hours'].sum()
        total_gdp = econ['summary_gdp'].sum()
        total_income = econ['summary_income'].sum()
        events['percent_of_gdp'] = (events['summary_gdp']/total_gdp*100).round(4)
        events['percent_of_income'] = (events['summary_income']/total_income*100).round(4)
        events['percent_of_events'] = (events['number_of_events']/total_events*100).round(4)
        events['percent_of_duration'] = (events['summary_duration']/total_duration*100).round(4)
        events['percent_of_customer_hours'] = (events['summary_customer_hours']/total_customer_hours*100).round(4)
        return events
    scopes = (['fips','state'])
    for scope in scopes:
        normal_df=normalize(scope)
        normal_df.to_sql(f"{scope}_normalized", con=connection, if_exists='replace', index=False)
    drop_by_index('outages.db', 'state_normalized', [0,3,4,5,6,7])
    drop_by_index('outages.db', 'fips_normalized', [1,3,4,6,7])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <h2>Visualizations</h2>
    """)
    return


@app.cell
def _(connection, pd, plt):
    #Marimo prohibits re-definition of variables across notebook cells. Visualization code is enclosed in anonymous functions for reusability.
    def _():
        graphquery = """
            select state, percent_of_customer_hours as 'Customer-Hours' , percent_of_duration as Duration, percent_of_events as 'Number of Events'
            from state_normalized
            order by percent_of_customer_hours desc
            limit 10
        """
        graph_df = pd.read_sql(graphquery, connection)
        graph_df.set_index('state', inplace=True)
        graph_df.plot(kind='bar')
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Electrical Outages (%)")
        plt.xlabel("")
        plt.title("Where Did Outages Most Often Occur?", fontsize=16, pad=20)

        ax = plt.gca()

        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots\OutagesPerStateBarPlot.png')
        plt.show()
    _()
    return


@app.cell
def _(connection, pd, plt):
    def _():
        graph_query = """
            select county, state2 as state, percent_of_customer_hours as 'Customer-Hours', percent_of_duration as Duration, percent_of_events as 'Number of Events' from fips_normalized
            order by percent_of_customer_hours desc
            limit 15
        """
        graph_df = pd.read_sql(graph_query, connection)
        graph_df['county']= graph_df['county'] + ", " + graph_df['state']
        graph_df.set_index('county', inplace=True)
        graph_df.plot(kind='bar')
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Electrical Outages (%)")
        plt.xlabel("")
        plt.title("Where Did Outages Most Often Occur?", fontsize=16, pad=20)

        ax = plt.gca()

        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots\OutagesPerCountyBarPlot.png')
        plt.show()

    _()
    return


@app.cell
def _(connection, pd, plt):
    def _():
        #plt.figure(figsize=(8,8))
        graph_query = """
        select type, round(sum(duration*mean_customers) * 100 / (select sum(duration*mean_customers) from outages)) as 'Customer-Hours' , round(sum(duration) * 100/ (select sum(duration) from outages)) as Duration, count(*) * 100 / (select count(*) from outages) as 'Number of Events'
        from outages
        group by type
        order by count(*) desc
        limit 3
        """
        graph_df = pd.read_sql(graph_query, connection)
        explode = [0, 0.1, 0.1]
        plt.pie(
            graph_df['Number of Events'],
            labels=graph_df['type'],
            explode=explode,
            autopct="%1.1f%%",
            #hatch=['**O', 'oO', 'O.O', '.||.'] #uncomment to make beautiful
                    )
        plt.title("Outage Events Overwhelmingly Resulted From Weather")

        plt.tight_layout()
        plt.savefig(r'plots/OutageEventTypesPieChart.png')
        return plt.show()


    _()
    return


@app.cell
def _(connection, pd, plt):
    def _():
        graph_query = """
            select county, state2 as state, percent_of_customer_hours as 'Outage Customer-Hours', percent_of_income as 'US Income' from fips_normalized
            order by percent_of_customer_hours desc
            limit 20
        """
        graph_df = pd.read_sql(graph_query, connection)
        graph_df['county']= graph_df['county'] + ", " + graph_df['state']
        graph_df.set_index('county', inplace=True)
        graph_df.plot(kind='bar')
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Percent")
        plt.xlabel("")
        plt.title("Wealth of Counties Where Outages Most Often Occurred", fontsize=16, pad=20)

        ax = plt.gca()
        #ax.set_yscale('log')
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots\CountyWealthvOutages.png')
        plt.show()

    _()
    return


@app.cell
def _(connection, np, pd, plt):
    def _():
        graph_query = """
        select state, percent_of_customer_hours, percent_of_gdp as GDP, percent_of_income as Income from state_normalized
        """
        graph_df = pd.read_sql(graph_query, connection)
        plt.Figure(figsize=(20,10))
        fig, ax = plt.subplots(figsize=(8, 6))
        for column in graph_df.columns[2:]:  # Skip the first two columns (X)
            ax.scatter(graph_df['percent_of_customer_hours'], graph_df[column], label=column)
         # Add trend line
        x = np.log10(graph_df['percent_of_customer_hours'])
        y = np.log10(graph_df[column])
    
        # Remove any NaN or inf values
        mask = np.isfinite(x) & np.isfinite(y)
        x_clean = x[mask]
        y_clean = y[mask]
    
        if len(x_clean) > 1:
            # Fit polynomial (degree 1 = linear)
            coeffs = np.polyfit(x_clean, y_clean, 1)
            poly = np.poly1d(coeffs)
        
            # Create trend line
            x_trend_log = np.array([x_clean.min(), x_clean.max()])
            y_trend_log = poly(x_trend_log)
        
            # Convert back from log
            x_trend = 10**x_trend_log
            y_trend = 10**y_trend_log
        
            # Calculate R-squared
            y_pred = poly(x_clean)
            ss_res = np.sum((y_clean - y_pred)**2)
            ss_tot = np.sum((y_clean - np.mean(y_clean))**2)
            r_squared = 1 - (ss_res / ss_tot)
        
            ax.plot(x_trend, y_trend, '--', linewidth=1.5,
                    label=f'{column} trend (R²={r_squared:.2f})')
    

        plt.ylabel("Percent of US Wealth")
        plt.xlabel("Percent of Outage Customer-Hours")
        plt.title("US State Wealth Weakly Correlates to Outage Time")
        ax.legend()
        ax.set_yscale('log')
        ax.set_xscale('log')

        for idx, row in graph_df.iterrows():
                ax.annotate(row['state'], # The label text from the 'labels' column
                            (row['percent_of_customer_hours'],row['Income']), # The x, y coordinates
                            textcoords="offset points", # Position the text relative to the point
                            xytext=(0, 5), # Offset the text by 5 points vertically
                            ha='center', # Center the text horizontally
                            fontsize=9) # Adjust font size
        plt.savefig(r'plots/StateWealthvCustomer-Hours.png')
        return plt.show()
    _()
    return


@app.cell
def _(connection, np, pd, plt):
    def _():
        graph_query = """
        select concat(county, ", ", state2) as county, percent_of_customer_hours, percent_of_gdp as GDP, percent_of_income as Income from fips_normalized 
        where percent_of_customer_hours > .3
        """
        graph_df = pd.read_sql(graph_query, connection)
        plt.Figure(figsize=(20,10))
        fig, ax = plt.subplots(figsize=(8, 6))
        for column in graph_df.columns[2:]:  # Skip the first two columns (X)
            ax.scatter(graph_df['percent_of_customer_hours'], graph_df[column], label=column)

        # Add trend line
        x = np.log10(graph_df['percent_of_customer_hours'])
        y = np.log10(graph_df[column])
    
        # Remove any NaN or inf values
        mask = np.isfinite(x) & np.isfinite(y)
        x_clean = x[mask]
        y_clean = y[mask]
    
        if len(x_clean) > 1:
            # Fit polynomial (degree 1 = linear)
            coeffs = np.polyfit(x_clean, y_clean, 1)
            poly = np.poly1d(coeffs)
        
            # Create trend line
            x_trend_log = np.array([x_clean.min(), x_clean.max()])
            y_trend_log = poly(x_trend_log)
        
            # Convert back from log
            x_trend = 10**x_trend_log
            y_trend = 10**y_trend_log
        
            # Calculate R-squared
            y_pred = poly(x_clean)
            ss_res = np.sum((y_clean - y_pred)**2)
            ss_tot = np.sum((y_clean - np.mean(y_clean))**2)
            r_squared = 1 - (ss_res / ss_tot)
        
            ax.plot(x_trend, y_trend, '--', linewidth=1.5,
                    label=f'{column} trend (R²={r_squared:.2f})')
    
    
        plt.xlabel("Percent of Outage Customer-Hours")
        plt.ylabel("Percent of US Wealth")
        plt.title("US County Wealth Weakly Correlates to Outage Time")
        ax.legend()
        ax.set_yscale('log')
        ax.set_xscale('log')
    
        for idx, row in graph_df.iterrows():
                    ax.annotate(row['county'], # The label text from the 'labels' column
                        (row['percent_of_customer_hours'],row['Income']), # The x, y coordinates
                        textcoords="offset points", # Position the text relative to the point
                        xytext=(0, 5), # Offset the text by 5 points vertically
                        ha='center', # Center the text horizontally
                        fontsize=9) # Adjust font size
        plt.savefig(r'plots/CountiesWealthvCustomer-Hours.png')
        return plt.show()
    _()
    return


@app.cell
def _(connection, np, pd, plt):

    def _():
        graph_query = """
            select sn.state, sum(amount), sum(percent) as 'FEMA Grants(%)', percent_of_gdp as GDP, percent_of_income as Income from fema f
            join state_normalized sn on sn.state = f.state  
            group by sn.state
            """
        graph_df = pd.read_sql(graph_query, connection)
        graph_df
        plt.Figure(figsize=(20,10))
        fig, ax = plt.subplots(figsize=(8, 6))
        for column in graph_df.columns[3:]:  # Skip the first three columns (X)
            ax.scatter(graph_df['FEMA Grants(%)'], graph_df[column], label=column)

        # Add trend line
        x = np.log10(graph_df['FEMA Grants(%)'])
        y = np.log10(graph_df[column])
    
        # Remove any NaN or inf values
        mask = np.isfinite(x) & np.isfinite(y)
        x_clean = x[mask]
        y_clean = y[mask]
    
        if len(x_clean) > 1:
            # Fit polynomial (degree 1 = linear)
            coeffs = np.polyfit(x_clean, y_clean, 1)
            poly = np.poly1d(coeffs)
        
            # Create trend line
            x_trend_log = np.array([x_clean.min(), x_clean.max()])
            y_trend_log = poly(x_trend_log)
        
            # Convert back from log
            x_trend = 10**x_trend_log
            y_trend = 10**y_trend_log
        
            # Calculate R-squared
            y_pred = poly(x_clean)
            ss_res = np.sum((y_clean - y_pred)**2)
            ss_tot = np.sum((y_clean - np.mean(y_clean))**2)
            r_squared = 1 - (ss_res / ss_tot)
        
            ax.plot(x_trend, y_trend, '--', linewidth=1.5,
                    label=f'{column} trend (R²={r_squared:.2f})')
    
        plt.xlabel("Percent of FEMA Grants")
        plt.ylabel("Percent of US Wealth")
        plt.title("FEMA Grants Weakly Correlates to Wealth")
        ax.legend()
        plt.ylim(.1, 100)
        plt.xlim(.1, 100)
        ax.set_yscale('log')
        ax.set_xscale('log')
        for idx, row in graph_df.iterrows():
                    ax.annotate(row['state'], # The label text from the 'labels' column
                        (row['FEMA Grants(%)'],row['Income']), # The x, y coordinates
                        textcoords="offset points", # Position the text relative to the point
                        xytext=(0, 5), # Offset the text by 5 points vertically
                        ha='center', # Center the text horizontally
                        fontsize=9) # Adjust font size
        plt.savefig(r'plots/GDPvFemaGrants.png')
        return plt.show()
    _()
    return


@app.cell
def _(connection, pd, plt):
    def _():
        graphquery = """
            select state, percent_of_gdp as 'US GDP', percent_of_customer_hours as 'Outage Customer-Hours'
            from state_normalized
            order by percent_of_gdp desc
            limit 4
        """
        graph_df = pd.read_sql(graphquery, connection)
        graph_df.set_index('state', inplace=True)
        graph_df.plot(kind='bar')
        total_cust_hrs = round(graph_df['Outage Customer-Hours'].sum())
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Percent")
        plt.xlabel("")
        plt.title(f"Wealthiest Four States Had {total_cust_hrs}% of All Outages", fontsize=16, pad=20)

        ax = plt.gca()

        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots\WealthPerStateBarPlot.png')
        plt.show()
    _()
    return


if __name__ == "__main__":
    app.run()
