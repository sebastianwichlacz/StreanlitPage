import re
import sqlite3

import pandas as pd
import requests
import streamlit as st
from pandas import json_normalize

st.set_page_config(page_title="Weld tracker", page_icon=":sparkles:", layout="wide")


def read_data_from_db(db_file, table_name):
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def format_robot_name(robot_name):
    robot_name = robot_name.replace("-", "").replace("SW", "").replace("MH", "")

    # if robot_name.startswith("FRM2"):
    #     prefix = robot_name[:4]  # FRM2
    #     rest = robot_name[4:]  # 0xxRBxx
    #     rest = re.sub(r"^0+", "", rest)
    #     robot_name = prefix + rest

    return robot_name


def printer(filtered_df):
    if filtered_df['Line'].eq('FRM2').all():
        schedule = str(filtered_df.iloc[0]['ProgNr'])
        api = f"http://10.95.62.243/4.1.0/timers/10_95_5_226_29/schedule/{schedule}"
    elif filtered_df['Line'].eq('LGT').all():
        schedule = str(filtered_df.iloc[0]['ProgNr'])
        api = f"http://10.95.62.247/4.1.0/timers/10_95_5_226_29/schedule/{schedule}"
    else:
        st.write("NOK")

    return api

def fetch_data_from_api(url, data_type, timeout=1):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx, 5xx)
        data = response.json()

        # Convert the data to a DataFrame if possible
        df = json_normalize(data, data_type)

        if df.empty:
            print(f"No data available for {data_type} at {url}")
            return None

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None


# ------------main-----------

def main():

    # Load data
    db_file = "db/database.db"
    df = read_data_from_db(db_file, "sw_summary")
    thickness_df = read_data_from_db(db_file, "thickness")
    weld_data_df = read_data_from_db(db_file, "weld_data")
    line_ips = read_data_from_db(db_file, 'line_ips')
    robot_ips = read_data_from_db(db_file, 'ips')

    merged_df = pd.merge(df, thickness_df, left_on='Point Name', right_on='point_id', how='left')
    merged_df[['Thickness', 'Material']] = merged_df['total_thk_mat'].str.split('//', expand=True).fillna('No data')
    merged_df['Thickness'] = merged_df['Thickness'].str.replace(',', '.')
    merged_df[['ProgNr', 'Force', 'Part Tolerance']] = merged_df[['ProgNr', 'Force', 'Part Tolerance']].fillna(0)
    merged_df[['ProgNr', 'Force', 'Part Tolerance']] = merged_df[['ProgNr', 'Force', 'Part Tolerance']].astype('int')


    # Header
    st.header('Spot Data')

    # Input for search
    search_term = st.text_input("Search:",
                                placeholder="Enter spot ID")


    # Filtering data based on the search term
    if search_term:

        # Filtering merged df based on point name
        filtered_df = merged_df[merged_df['Point Name'].str.contains(search_term, na=False)]


        if filtered_df.shape[0] > 1:
            st.write(filtered_df[['Point Name', 'ProgNr']])


        # Filtering data for api url
        line_name = filtered_df['Line'].iloc[0]
        robot_name = format_robot_name(filtered_df['RobotName'].iloc[0])
        if line_name == 'FRM1':
            if int(robot_name[5]) <= 3:
                line_name = 'FRM1_1'
            elif int(robot_name[5]) > 3:
                line_name = 'FRM1_2'

        line = line_ips['ip'][line_ips['line'] == line_name].iloc[0]
        robot_ip = robot_ips['ip'][robot_ips['robot_name'] == robot_name].iloc[0].replace('.','_')
        robot_id = robot_ips['robot_id'][robot_ips['robot_name'] == robot_name].iloc[0]
        schedule_numb = filtered_df['ProgNr'].iloc[0]

        # API history link
        history_url = f"http://{line}/4.1.0/timers/{robot_ip}_{robot_id}/history/weld/schedule/{schedule_numb}"

        # API schedule link
        api = f"http://{line}/4.1.0/timers/{robot_ip}_{robot_id}/schedule/{schedule_numb}"

        # History df
        weld_data_filtered = fetch_data_from_api(history_url, 'history')

        # columns
        left_column, middle_column, right_column = st.columns([2, 2, 2])

        # get response
        response = requests.get(api)

        if response.status_code == 200:
            data = response.json()
        else:
            st.write(f"Failed to retrieve data: {response.status_code}")

        # converting to df
        schedule_df = json_normalize(data, 'schedule')

        # function lists
        vs_list = ['22', '23', '24']
        normal_list = ['32', '33', '34']

        # 1608441-S-0750
        st.markdown("""
            <style>
            .st-emotion-cache-ocqkz7{
            gap: 0rem !important;
            }
            .stDivider {
                margin-top: 0rem;
                margin-bottom: 0rem;
            }
            </style>
            """, unsafe_allow_html=True)

        if filtered_df.iloc[0]['Manufacturor'] == 'KUKA':
            # 1503000-S-2960
            color = '#F92300'
        else:
            # 1687777-S-0444
            color = '#F9D200'

        # left column
        with left_column:

            # Manufacturer
            st.markdown(
                f"<span style='font-size:20px;'><b>Manufacturer:</b>"
                f" <span style='color:{color};'>{filtered_df.iloc[0]['Manufacturor'] if not filtered_df.empty else 'N/A'}</span>",
                unsafe_allow_html=True)

            # Robot name
            st.markdown(
                f"<span style='font-size:20px;'><b>Robot name:</b>"
                f" <span style='color:{color};'>{filtered_df.iloc[0]['RobotName'] if not filtered_df.empty else 'N/A'}</span>",
                unsafe_allow_html=True)


            # Program number
            st.markdown(
                f"<span style='font-size:20px;'><b>Program number:</b>"
                f" <span style='color:{color};'>{filtered_df.iloc[0]['ProgNr'] if not filtered_df.empty else 'N/A'}</span>",
                unsafe_allow_html=True)


            st.markdown(
                f"<span style='font-size:20px;'><b>Turns ratio:</b>"
                f" <span style='color:{color};'>{weld_data_filtered.iloc[0]['turnsratio'] if not weld_data_df.empty else 'N/A'}</span>",
                unsafe_allow_html=True
            )





        with middle_column:
            # pre weld
            if schedule_df['function'].isin(vs_list).any():
                preweld = schedule_df[schedule_df['function'].isin(vs_list)]
                if not preweld.empty:
                    time = preweld.iloc[0]['param_one']
                    current = preweld.iloc[0]['param_two']
            # 1504000-S-0903
            elif schedule_df['function'].isin(normal_list).any():
                preweld = schedule_df[schedule_df['function'].isin(normal_list)]
                if not preweld.empty:
                    time = preweld.iloc[0]['param_one']
                    current = preweld.iloc[0]['param_two']

                st.markdown(
                    f"<span style='font-size:20px;'><b>Pre-weld:</b>"
                    f" <span style='color:{color};'>{time}ms, {current}0A</span>",
                    unsafe_allow_html=True
                )
                # st.markdown(
                #     f"<span style='font-size:20px;'><b>Pre-weld current:</b>"
                #     f" <span style='color:{color};'></span>",
                #     unsafe_allow_html=True
                # )


            # Slope up
            if schedule_df['function'].eq('45').any():
                slope = schedule_df[schedule_df['function'].eq('45')]
                if not slope.empty and slope.iloc[0]['param_two'] < slope.iloc[0]['param_three']:
                    time = slope.iloc[0]['param_one']
                    start_current = slope.iloc[0]['param_two']
                    end_current = slope.iloc[0]['param_three']

                    st.markdown(
                        f"<span style='font-size:20px;'><b>Slope up:</b>"
                        f" <span style='color:{color};'>{time}ms, {start_current}0A - {end_current}0A</span>",
                        unsafe_allow_html=True
                    )

            # main weld
            if schedule_df['function'].eq('60').any():
                if schedule_df['function'].eq('30').any():
                    impuls = schedule_df[schedule_df['function'].eq('60')]
                    heat = str(impuls.iloc[0]['param_one'])
                    cool = str(impuls.iloc[0]['param_two'])

                    weld = schedule_df[schedule_df['function'].eq('30')]
                    if not weld.empty:
                        time = str(weld.iloc[0]['param_one'])
                        if len(time) == 1:
                            time = time + 'x'
                        else:
                            time = time + 'ms'
                        current = weld.iloc[0]['param_two']
                    st.markdown(
                        f"<span style='font-size:20px;'><b>Weld time:</b>"
                        f" <span style='color:{color};'>{time} {heat}ms</span>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"<span style='font-size:20px;'><b>Cool:</b>"
                        f" <span style='color:{color};'>{cool}ms</span>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"<span style='font-size:20px;'><b>Weld current:</b>"
                        f" <span style='color:{color};'>{current}0A</span>",
                        unsafe_allow_html=True
                    )
            else:
                if schedule_df['function'].eq('30').any():
                    weld = schedule_df[schedule_df['function'].eq('30')]
                    if not weld.empty:
                        time = str(weld.iloc[0]['param_one'])
                        if len(time) == 1:
                            time = time + 'x'
                        else:
                            time = time + 'ms'
                        current = weld.iloc[0]['param_two']
                    st.markdown(
                        f"<span style='font-size:20px;'><b>Weld:</b>"
                        f" <span style='color:{color};'>{time}, {current}0A</span>",
                        unsafe_allow_html=True
                    )



            if schedule_df['function'].eq('45').any():
                slope = schedule_df[schedule_df['function'].eq('45')]
                if not slope.empty and slope.iloc[0]['param_two'] > slope.iloc[0]['param_three']:
                    time = slope.iloc[0]['param_one']
                    start_current = slope.iloc[0]['param_two']
                    end_current = slope.iloc[0]['param_three']

                    st.markdown(
                        f"<span style='font-size:20px;'><b>Slope down:</b>"
                        f" <span style='color:{color};'>{time}ms, {start_current}0A - {end_current}0A</span>",
                        unsafe_allow_html=True
                    )

                elif not slope.empty and len(slope) > 1:
                    time2 = slope.iloc[1]['param_one']
                    start_current2 = slope.iloc[1]['param_two']
                    end_current2 = slope.iloc[1]['param_three']

                    st.markdown(
                        f"<span style='font-size:20px;'><b>Slope down:</b>"
                        f" <span style='color:{color};'>{time2}ms, {start_current2}0A - {end_current2}0A</span>",
                        unsafe_allow_html=True
                    )






        # Right column
        with right_column:


            # Thickness
            thickness = filtered_df.iloc[0]['Thickness']
            if not thickness == 'No data':
                st.markdown(
                    f"<span style='font-size:20px;'><b>Thickness:</b>"
                    f" <span style='color:{color};'>{thickness if not filtered_df.empty else 'N/A'}</span>",
                    unsafe_allow_html=True)
            # Materials
            materials = filtered_df.iloc[0]['Material']
            if not materials == 'No data':
                st.markdown(
                    f"<span style='font-size:20px;'><b>Materials:</b>"
                    f" <span style='color:{color};'>{materials if not filtered_df.empty else 'N/A'}</span>",
                    unsafe_allow_html=True)

            # Total thickness
            st.markdown(
                f"<span style='font-size:20px;'><b>Total thickness:</b>"
                f" <span style='color:{color};'>{filtered_df.iloc[0]['PartThickness']  if not filtered_df.empty else 'N/A'} ± {filtered_df.iloc[0]['Part Tolerance']}</span>",
                unsafe_allow_html=True)


            # Force
            st.markdown(
                f"<span style='font-size:20px;'><b>Force:</b> "
                f"<span style='color:{color};'>{filtered_df.iloc[0]['Force'] if not filtered_df.empty else 'N/A'}N</span>",
                unsafe_allow_html=True)




        # Resistance sum D

        if not weld_data_filtered['ressumd'].eq(0).all():
            d_sum_data = weld_data_filtered['ressumd'].tolist()
            st.subheader("Resistance sum D")
            amount_input = st.slider("Choose amount of data which you want to display:", 0,100,(50))

            st.line_chart(d_sum_data[:amount_input])



if __name__ == "__main__":
    main()
