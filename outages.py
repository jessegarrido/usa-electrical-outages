import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    #Imports
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    import datetime
    import sqlite3

    return np, pd, plt, sqlite3


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Read in data from csv
    """)
    return


@app.cell
def _(pd):
    #Read in Department of Energy outage data
    df = pd.read_csv(r'data\eaglei_outages_with_events_2022.csv')
    df2 = pd.read_csv(r'data\eaglei_outages_with_events_2023.csv')
    df = pd.concat([df,df2])
    df['Datetime Event Began'] = pd.to_datetime(df['Datetime Event Began'], errors='coerce')
    df['Datetime Event Began'] = df['Datetime Event Began'].dt.tz_localize('UTC')
    df['event_id']=df['event_id'] + "-" + df['Datetime Event Began'].dt.year.astype(str)
    df.head()
    df
    return (df,)


@app.cell
def _(df, pd):
    df['Event Type'].describe()
    print()
    print(df['Event Type'].unique())
    human_keywords = "attack|suspicious|theft|vandalism|cyber|sabotage"
    system_keywords = "failure|fuel|operations|interruption|inadequacy"
    system_exclude_keywords = "attack|unknown"
    def parse_event(event):
        event = pd.Series(event.lower())
        if event.str.contains("weather").any():
            return "Weather"
        if event.str.contains(human_keywords).any():
            return "Human Intervention"
        if event.str.contains(system_keywords).any() & ~event.str.contains(system_exclude_keywords).any():
            return "System Failure"
        return "Unknown"
    df['Event']=df['Event Type'].apply(parse_event)
    print(df['Event'].value_counts())
    print("\n\nWEATHER:",df[df['Event']=="Weather"]['Event Type'].unique(),
        "\n\nHUMAN INTERVENTION:",df[df['Event']=="Human Intervention"]['Event Type'].unique(),
        "\n\nSYSTEM FAILURES:",df[df['Event']=="System Failure"]['Event Type'].unique(),
        "\n\nUNKNOWN:",df[df['Event']=="Unknown"]['Event Type'].unique())
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
    econ_df
    return df3, econ_df


@app.cell
def _(econ_df, pd):
    econ_facts_df = pd.DataFrame()
    # Using iterrows()
    for index, row in econ_df.iterrows():
      #  new_row = [len(econ_facts_df),econ_facts_df]
        for year in range(2022,2024):
            new_row = pd.DataFrame({'econ_id': [len(econ_facts_df)], 'fips': [row['fips']],'year': [year],'income':row[f'income{year}'],'gdp':row[f'gdp{year}']})
            econ_facts_df = pd.concat([econ_facts_df,new_row], ignore_index=True)
    econ_facts_df
    return (econ_facts_df,)


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
    #counties_df['name']=None
    counties_df.loc[:, 'state']=counties_df.name.str.split(',').str[-1]
    counties_df.loc[:, 'name']=counties_df.name.str.split(',').str[0] 

    # Use .loc to explicitly set values on the original DataFrame
    counties_df.loc[:, 'state'] = counties_df['state'].replace('\\*', '', regex=True)

    counties_df.loc[:, 'state'] = (counties_df['state'].astype(str)
            .str.strip() 
            .map(state_abbreviations) 
            .fillna('Unknown')
    )
    counties_df
    return (counties_df,)


@app.cell
def _(pd):
    #FEMA 
    fema_df = pd.read_csv(r'data\PublicAssistanceFundedProjectsSummaries.csv')
    fema_df=fema_df.iloc[:,[0,1,2,3,5,8]]
    fema_df=fema_df.dropna(subset=['declarationDate'])
    fema_df['Date']=pd.to_datetime(fema_df['declarationDate'])
    fema_df = fema_df[fema_df['Date'].dt.year > 2021]
    fema_df.head()
    fema_df
    return (fema_df,)


@app.cell
def _(df):
    events_df=df.drop_duplicates('event_id')
    events_df=events_df.iloc[:,[0,1,2,14]]
    events_df
    return (events_df,)


@app.cell
def _(fema_df):
    disasters_df=fema_df.drop_duplicates('disasterNumber')
    disasters_df=disasters_df.iloc[:,[0,3,1,2]]
    disasters_df
    return (disasters_df,)


@app.cell
def _(disasters_df, events_df, pd):
    disasters_df['declarationDate'] = pd.to_datetime(disasters_df['declarationDate'], errors='coerce')
    disasters_df['event_id'] = None
    disasters_df['Datetime Event Began'] = None
    for pos1, (_, row1) in enumerate(disasters_df.iterrows()):
        for pos2, (_, row2) in enumerate(events_df.iterrows()):
            diff_days = row1['declarationDate'] - row2['Datetime Event Began']
            if (row2['state_event'] == row1['state']) & (diff_days.days <= 20) & (diff_days.days >= 0) & (row2['Event'] == 'Weather'):
                disasters_df.iat[pos1,-2] = row2['event_id']
                disasters_df.iat[pos1,-1] = row2['Datetime Event Began']
    disasters_df
    return


@app.cell
def _(disasters_df, fema_df, pd):
    fema_df2 = pd.merge(fema_df,disasters_df,on="disasterNumber", how="left")
    fema_df3=fema_df2.groupby('event_id')['federalObligatedAmount'].sum()
    fema_df3
    return (fema_df3,)


@app.cell
def _(disasters_df, fema_df3, pd):
    fema_facts_df = pd.merge(fema_df3,disasters_df,on="event_id", how="left")
    fema_facts_df['disaster_id']=fema_facts_df.index+1
    fema_facts_df=fema_facts_df.iloc[:,[7,0,4,5,3,1]]
    fema_facts_df.columns=['disaster_id','name','date','type','state','amount']
    fema_facts_df
    return (fema_facts_df,)


@app.cell
def _(df, fema_facts_df, pd):
    events_table_df = pd.merge(df,fema_facts_df,how="left",left_on='event_id',right_on='name')
    events_table_df['outage_id']=events_table_df.index+1
    events_table_df=events_table_df.iloc[:,[-1,15,4,5,6,7,8,10,9,11,12,13]]
    events_table_df.columns=['outage_id','disaster_id','type','fips','state','county','start_time','end_time','duration','min_customers','max_customers','mean_customers']
    events_table_df
    return (events_table_df,)


@app.cell
def _(econ_facts_df):
    econ_facts_df
    return


@app.cell
def _(sqlite3):
    connection = sqlite3.connect("outages.db")
    connection
    return (connection,)


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
def _(connection, mo):
    _df = mo.sql(
        f"""
        select * from outages
        """,
        engine=connection
    )
    return


app._unparsable_cell(
    r"""
    select {scope}, count(*) as number_of_events, sum(duration) as summary_duration from outages o
    on strftime('%Y', o.start_time) = e.year AND o.fips=e.fips
    group by {scope}
    order by number_of_events desc
    """,
    name="_"
)


@app.cell
def _(connection, pd):
    def normalize(scope):
        query1 = f""" 
            select c.state as state2, c.name as county, o.{scope}, avg(gdp) as summary_avg_gdp, avg(income) as summary_avg_income, count(*) as number_of_events, sum(duration * mean_customers) as summary_customer_hours, sum(duration) as summary_duration from outages o
            join econ_facts e on strftime('%Y', o.start_time) = e.year AND o.fips=e.fips
            join counties c on c.fips = o.fips
            group by o.{scope}, c.state
            order by number_of_events desc
        """
        query2 = f"""
            select c.{scope}, avg(gdp) over (partition by c.{scope}) as summary_avg_gdp, avg(income) over (partition by c.{scope}) as summary_avg_income from econ_facts e
            join counties c on c.fips = e.fips
            group by c.{scope}
        """
        events = pd.read_sql(query1, connection)
        econ = pd.read_sql(query2, connection)
        total_events = events['number_of_events'].sum()
        total_duration = events['summary_duration'].sum()
        total_customer_hours = events['summary_customer_hours'].sum()
        total_gdp = econ['summary_avg_gdp'].sum()
        total_income = econ['summary_avg_income'].sum()
        events['percent_of_gdp'] = (events['summary_avg_gdp']/total_gdp*100).round(4)
        events['percent_of_income'] = (events['summary_avg_income']/total_income*100).round(4)
        events['percent_of_events'] = (events['number_of_events']/total_events*100).round(4)
        events['percent_of_duration'] = (events['summary_duration']/total_duration*100).round(4)
        events['percent_of_customer_hours'] = (events['summary_customer_hours']/total_customer_hours*100).round(4)
        #print(econ)
        return events
    scopes = (['state','fips'])
    for scope in scopes:
        normal_df=normalize(scope)
        #print(normal_df)
        normal_df.to_sql(f"{scope}_summary", con=connection, if_exists='replace', index=False)
    return


@app.cell
def _(connection, pd):
    pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", connection)
    return


@app.cell(hide_code=True)
def _(connection, mo):
    _df = mo.sql(
        f"""
        select * from state_summary
        """,
        engine=connection
    )
    return


@app.cell
def _(bystate_df2, plt):
    bystate_df2.plot(kind='bar')
    plt.xticks(rotation=45, ha="right", fontsize=8)
    plt.ylabel("Electrical Outages (%)")
    plt.xlabel("")
    plt.title("Where Did Outages Occur?", fontsize=16, pad=20)

    ax = plt.gca()
    for spine in ax.spines.values():
        spine.set_linewidth(0.5)
        spine.set_alpha(0.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(r'plots\OutagesPerStateBarPlot.png')
    plt.show()
    return


@app.cell
def _(byevent_df2, plt):
    def _():
        byevent_df2.plot(kind='bar')
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Electrical Outages (%)")
        plt.xlabel("")
        plt.title("What Caused Electrical Outages?", fontsize=16, pad=20)

        ax = plt.gca()
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots/OutagesPerEventTypeBarPlot.png')
        return plt.show()


    _()
    return


@app.cell
def _(byevent_df2, plt):
    def _():
        byevent_df2.plot(kind='bar')
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.ylabel("Electrical Outages (%)")
        plt.xlabel("")
        plt.title("What Caused Electrical Outages?", fontsize=16, pad=20)

        ax = plt.gca()
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
            spine.set_alpha(0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        plt.savefig(r'plots/OutagesPerEventTypeBarPlot.png')
        return plt.show()


    _()
    return


@app.cell
def _(byevent_df2, events, plt):
    #plt.figure(figsize=(8,8))
    plt.pie(
        byevent_df2['Events'],
        labels=None,
        startangle=110,
        autopct="%1.1f%%",
        #hatch=['**O', 'oO', 'O.O', '.||.'] #uncomment to make beautiful
                )
    plt.title("Outages Overwhelmingly Result From Weather")
    plt.legend(
        labels = events.index,
        loc="lower right",
    )

    plt.tight_layout()
    plt.savefig(r'plots/OutageEventTypesPieChart.png')
    plt.show()
    return


@app.cell
def _(df, np, plt):
    def _():
        plt.Figure(figsize=(10,6))
        # Calculate the best-fit line
        x=df["duration"]/24
        y=df["max_customers"]
        z = np.polyfit(x,y , 1)
        p = np.poly1d(z)
        plt.scatter(
                    x, 
                    y,
                    color = "#D4D4D4",
                    alpha = 0.7,
                    s = 10
                    )
        plt.plot(x, p(x), "r--", label="Linear Trend Line")  # 'r--' is for a red dashed line         
        plt.ylim(1, 150000)
        plt.xlabel("Outage Duration(d)")
        plt.ylabel("Peak Customers Impacted")

        ax = plt.gca()
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        #ax.set_xscale('log')
        #ax.set_yscale('log')

        ax.set_title("Customers Impacted Rises With Outage Duration", fontsize = 18, pad=35)

        for spine in ax.spines.values():
            spine.set_linewidth(0.25)
            spine.set_alpha(0.5)
        plt.tight_layout()
        plt.savefig(r'plots/NumberImpactedVDuration.png')
        plt.legend()
        return plt.show()


    _()
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
