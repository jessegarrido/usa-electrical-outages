import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")


@app.cell
def _():
    #Imports
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    #Read in Department of Energy outage data
    df = pd.read_csv(r'data\eaglei_outages_with_events_2023.csv')
    df.head()
    df
    return df, np, pd, plt


@app.cell
def _(df):
    df['event_id'].unique()
    return


@app.cell
def _(pd):
    #Annual Income Data from Bureau of Economic Analysis
    df2 = pd.read_csv(r'data\CAINC1.csv')
    df2.columns=['fips','name','income2022','income2023','income2024']
    df3 = pd.read_csv(r'data\CAGDP1.csv')
    df3.columns=['fips','name','gdp2022','gdp2023','gdp2024']
    econ_df=pd.merge(df2,df3,on=["fips","name"])

    #econ_df.head()
    #econ_df[econ_df['fips'].isnull()]
    econ_df = econ_df[econ_df['fips'].notna()]
    #econ_df['fips']=econ_df['fips'].fillna(0)
    econ_df['fips']=pd.to_numeric(econ_df['fips'], errors='coerce')
    econ_df
    return (econ_df,)


@app.cell
def _(pd):
    #FEMA 
    fema_df = pd.read_csv(r'data\PublicAssistanceFundedProjectsSummaries.csv' \
    '')
    fema_df=fema_df.iloc[:,[1,2,3,5,8]]
    #df2.columns=['fips','name','income2022','income2023','income2024']
    #fema_df=pd.merge(df2,df3,on=["fips","name"])
    fema_df=fema_df.dropna(subset=['declarationDate'])
    fema_df['Date']=pd.to_datetime(fema_df['declarationDate'])
    fema_df = fema_df[fema_df['Date'].dt.year > 2022]
    fema_df.head()
    fema_df
    return (fema_df,)


@app.cell
def _(fema_df):
    fema_df['incidentType'].unique()
    return


@app.cell
def _(df, econ_df):
    merged_df = df.merge(econ_df,on='fips')
    merged_df[['fips','event_id','Event Type','county','income2023','gdp2023']][(merged_df['fips'] == 21111)]
    return


@app.cell
def _(df):
    df['Event Type'].describe()
    print()
    print(df['Event Type'].unique())
    return


@app.cell
def _(df, pd):
    human_keywords = "attack|suspicious|theft|vandalism|cyber"
    system_keywords = "failure|fuel|operations|interruption"
    system_exclude_keywords = "attack|unknown|other"
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
    return


@app.cell
def _(df):
    #print(df['Event'].value_counts())
    print("\n\nWEATHER:",df[df['Event']=="Weather"]['Event Type'].unique(),
        "\n\nHUMAN INTERVENTION:",df[df['Event']=="Human Intervention"]['Event Type'].unique(),
        "\n\nSYSTEM FAILURES:",df[df['Event']=="System Failure"]['Event Type'].unique(),
        "\n\nUNKNOWN:",df[df['Event']=="Unknown"]['Event Type'].unique())
    return


@app.cell
def _(df):
    df.describe().round(2)
    return


@app.cell
def _(df, pd):
    #count and normalize Events per state
    events = df['state'].value_counts()
    events_df = pd.DataFrame(events.rename_axis('State').reset_index(name='Event Count'))
    sum_events = df['state'].value_counts().sum()
    events_pu_df = events_df.copy()
    events_pu_df['Events'] = pd.DataFrame(events_df['Event Count'].apply(lambda x: x/sum_events*100))

    #count and normalize Duration per state
    duration = df.groupby('state')['duration'].sum()
    duration_df = pd.DataFrame(duration.rename_axis('State').reset_index(name='Duration(h)'))
    sum_duration = duration_df['Duration(h)'].sum()
    duration_pu_df = duration_df.copy()
    duration_pu_df['Duration'] = pd.DataFrame(duration_df['Duration(h)'].apply(lambda x:x/sum_duration*100))

    #count and normalize customer-hours per state
    df['customer-hours']=(df['min_customers']+df['max_customers'])/2*df['duration']
    csthrs = df.groupby('state')['customer-hours'].sum()
    csthrs_df = pd.DataFrame(csthrs.rename_axis('State').reset_index(name='customer-hours'))
    csthrs_df
    sum_csthrs = df['customer-hours'].sum()
    csthrs_pu_df = csthrs_df.copy()
    csthrs_pu_df['Customer-hours'] = pd.DataFrame(csthrs_df['customer-hours'].apply(lambda x: x/sum_csthrs*100))

    #merge dfs
    bystate_df=events_pu_df.merge(duration_pu_df)
    bystate_df=bystate_df.merge(csthrs_pu_df)

    #drop not-normalized columns and minor states
    bystate_df2=bystate_df.drop(['Event Count','Duration(h)','customer-hours'],axis=1)
    bystate_df2=bystate_df2[bystate_df2['Events']>1]
    bystate_df2.set_index('State',inplace=True)
    #print(bystate_df2)

    #count and normalize Count per Event
    events = df['Event'].value_counts()
    events_df = pd.DataFrame(events.rename_axis('Event').reset_index(name='Event Count'))
    sum_events = df['Event'].value_counts().sum()
    events_pu_df = events_df.copy()
    events_pu_df['Events'] = pd.DataFrame(events_df['Event Count'].apply(lambda x: x/sum_events*100))

    #count and normalize Duration per Event
    duration = df.groupby('Event')['duration'].sum()
    duration_df = pd.DataFrame(duration.rename_axis('Event').reset_index(name='Duration(h)'))
    sum_duration = duration_df['Duration(h)'].sum()
    duration_pu_df = duration_df.copy()
    duration_pu_df['Duration'] = pd.DataFrame(duration_df['Duration(h)'].apply(lambda x:x/sum_duration*100))

    #count and normalize customer-hours per Event
    df['customer-hours']=(df['min_customers']+df['max_customers'])/2*df['duration']
    csthrs = df.groupby('Event')['customer-hours'].sum()
    csthrs_df = pd.DataFrame(csthrs.rename_axis('Event').reset_index(name='customer-hours'))
    sum_csthrs = df['customer-hours'].sum()
    csthrs_pu_df = csthrs_df.copy()
    csthrs_pu_df['Customer-hours'] = pd.DataFrame(csthrs_df['customer-hours'].apply(lambda x: x/sum_csthrs*100))

    #merge dfs
    byevent_df=events_pu_df.merge(duration_pu_df)
    byevent_df=byevent_df.merge(csthrs_pu_df)

    #drop not-normalized columns
    byevent_df2=byevent_df.drop(['Event Count','Duration(h)','customer-hours'],axis=1)
    byevent_df2.set_index('Event',inplace=True)
    print(bystate_df2)
    print("\n\n")
    print(byevent_df2)
    return byevent_df2, bystate_df2, events


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


if __name__ == "__main__":
    app.run()
