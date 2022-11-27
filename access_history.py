# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
from streamlit_echarts import st_echarts
import pandas as pd

# Lookback should avoid cutting off hours, so always a full month/day/hour
config = {
    "Monthly": {
        "lookback": "date_trunc('MONTH', dateadd(month, -12, getdate()))",
        "range": "date_trunc('MONTH', query_start_time)",
        "slider_index": -3,
    },
    "Daily": { 
        "lookback": "date_trunc('DAY', dateadd(day, -31, getdate()))",
        "range": "date_trunc('DAY', query_start_time)",
        "slider_index": -15,
    },
    "Hourly": {
        "lookback": "date_trunc('HOUR', dateadd(hour, -72, getdate()))",
        "range": "date_trunc('HOUR', query_start_time)",
        "slider_index": -25,
    },
    "environments": ('PRD', 'TST', 'DEV'), # Needs to match with environments in 'myenv.fn' and 'myns.fn'
    "durations": ('Hourly', 'Daily', 'Monthly'),
    "myenv": "demo_db.public.myenv",
    "myns":  "demo_db.public.myns",
    "exclude_users": "('SYSTEM')",
}

# Create Session object
def create_session_object():
    connection_parameters = {
      "account":   "***",
      "user":      "***",
      "password":  "***",
      "role":      "***",
      "warehouse": "***",
      "database":  "SNOWFLAKE",
      "schema":    "ACCOUNT_USAGE"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

#@st.cache
def load_usage_data(session, range_option, env_option, table):
    snow_df_table = session.sql(f"""
    select count(*) as cnt, 
           {config['myns']}(f1.value:"objectName"::string) as ns,
           {config[range_option]['range']} as time_unit
      from access_history ah
           , lateral flatten({table}) f1
     where f1.value:"objectId" is not null
       and query_start_time >= {config[range_option]['lookback']}
       and f1.value:"objectDomain"::string != 'Stage'
       and {config['myenv']}(f1.value:"objectName"::string) = '{env_option}'
       and user_name not in {config['exclude_users']}
     group by ns, time_unit
     order by time_unit asc
    """)
    undly_use_pd = snow_df_table.to_pandas()

    columns = []
    leg_usage = {}
    for row in undly_use_pd['NS']:
        leg_usage[row] = 1
    for key in leg_usage.keys():
        columns.append(key)
    columns.sort()
    # pivot table will be easier to consume
    pivot_columns = "'" + "','".join(columns) + "'"
    pretty_columns = ','.join(columns).lower()
    snow_df_pivot = session.sql(f"""
    select * from table(result_scan(last_query_id()))
             pivot(sum(cnt) for ns in ({pivot_columns}))
          as p (time_unit, {pretty_columns})
     order by time_unit asc
    """)
    pivot_pd = snow_df_pivot.to_pandas()
    pivot_pd.fillna(0, inplace=True)
    return pivot_pd, columns

def load_underlying_data(session, range_option, env_option, table, undly_source):
    snow_df_table = session.sql(f"""
    select count(*) as cnt, 
           replace(f1.value:"objectName"::string, '{undly_source}_{env_option}_', '') as TN,
           {config[range_option]['range']} as time_unit
      from access_history ah
           , lateral flatten({table}) f1
     where f1.value:"objectId" is not null
       and query_start_time >= {config[range_option]['lookback']}
       and f1.value:"objectDomain"::string != 'Stage'
       and {config['myenv']}(f1.value:"objectName"::string) = '{env_option}'
       and {config['myns']}(f1.value:"objectName"::string) = '{undly_source}'
       and user_name not in {config['exclude_users']}
     group by TN, time_unit
     order by time_unit asc
    """)
    undly_use_pd = snow_df_table.to_pandas()

    columns = []
    leg_usage = {}
    for row in undly_use_pd['TN']:
        leg_usage[row] = 1
    for key in leg_usage.keys():
        columns.append(key)
    columns.sort()
    # pivot table will be easier to consume
    pivot_columns = "'" + "','".join(columns) + "'"
    pretty_columns = '"' + '","'.join(columns).upper() + '"'
    snow_df_pivot = session.sql(f"""
    select * from table(result_scan(last_query_id()))
             pivot(sum(cnt) for tn in ({pivot_columns}))
          as p (time_unit, {pretty_columns})
     order by time_unit asc
    """)
    pivot_pd = snow_df_pivot.to_pandas()
    pivot_pd.fillna(0, inplace=True)
    quoted_columns = []
    for key in leg_usage.keys():
        quoted_columns.append('"' + key + '"')
    quoted_columns.sort()
    return pivot_pd, quoted_columns

def show_stacked_area_graph(df, cols, range_option, title):
    xAxis = []
    slider_index = config[range_option]['slider_index']
    if range_option == 'Daily':
        for day in df['TIME_UNIT']:
            day_str = '%s-%s-%s' % (day.year, day.month, day.day)
            xAxis.append(day_str)
    elif range_option == 'Hourly': 
        for hour in df['TIME_UNIT']:
            hour_str = '%s-%s-%s %s:00' % (hour.year, hour.month, hour.day, hour.hour)
            xAxis.append(hour_str)
    elif range_option == 'Monthly':
        for month in df['TIME_UNIT']:
            month_str = '%s-%s' % (month.year, month.month)
            xAxis.append(month_str)
    if len(xAxis) + slider_index < 0:
        slider_index = 0
    
    series = []
    for col in cols:
        data = []
        for row in df[col]:
            data.append(row)
        series.append({ "name": col, "type": "line", "stack": "stack", "areaStyle": {}, "emphasis": {"focus": "series"}, "data": data})
    
    st.header(title)
    # Todo: LEGEND should be largest sum-> smallest sum
    # Todo: make tooltip/label remove elements with 0
    options = {
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "line", # simple line instead of cross
                "label": { "backgroundColor": "#6a7985" }, # Color of the highlighted label on xAxis
                "axis": "x", # only display xAxis line
            },
            "order": "valueAsc",
            "appendToBody": True,
        },
        "legend": { 
            "data": cols,
            "type": "scroll",
        },
        "grid": { "left": "3%", "right": "4%", "bottom": "12%", "containLabel": True },
        "xAxis": [
             {
                "type": "category",
                "axisTick": { "alignWithLabel": True },
                "boundaryGap": False,
                "data": xAxis,
                "axisPointer": { "label": { "show": True } }, 
            }
        ],
        "yAxis": [{"type": "value"}],
        "series": series,
    }
    # we need to not even display the datazoom if len(xAxis) == 0
    if len(xAxis) > 0:
        options['dataZoom'] = [ { "startValue": xAxis[slider_index] }, { "type": "inside" } ]
    st_echarts(options=options, height="400px", key=title)
    
if __name__ == "__main__":
    st.set_page_config(
        page_title="Visualization of Indirect/Direct Data Access in Snowflake",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="auto",
        menu_items={
            'Get Help': 'https://github.com/thomaseibner/streamlit-snowflake-access-history',
            'About': "Python & Streamlit app to display ❄️ access_history data"
        }
    )
    session = create_session_object()
    st.header('Visualization of Indirect/Direct Data Access in Snowflake')
    col1, col2 = st.columns(2)
    with col1:
        env_option = st.selectbox('Environment', config['environments'])
    with col2:
        range_option = st.selectbox(
            'Duration',
            config['durations'])
    
    df_usage, usage_columns = load_usage_data(session, range_option, env_option, 'base_objects_accessed')
    show_stacked_area_graph(df_usage, usage_columns, range_option, "Underlying Data Egress")
    df_egress, egress_columns = load_usage_data(session, range_option, env_option, 'direct_objects_accessed')
    show_stacked_area_graph(df_egress, egress_columns, range_option, "Direct Data Egress")

    col3, col4 = st.columns(2)
    with col3:
        undly_option = st.selectbox('Underlying Data Source', usage_columns)
    with col4:
        direct_option = st.selectbox('Direct Data Source', egress_columns)

    df_undly, undly_columns = load_underlying_data(session, range_option, env_option, 'base_objects_accessed', undly_option)
    show_stacked_area_graph(df_undly, undly_columns, range_option, f"{undly_option} Underlying Data Egress")
    df_direct, direct_columns = load_underlying_data(session, range_option, env_option, 'direct_objects_accessed', direct_option)
    show_stacked_area_graph(df_direct, direct_columns, range_option, f"{direct_option} Direct Data Egress")

    