import sqlite3
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from pandas import json_normalize
from streamlit_extras.let_it_rain import rain


def read_data_from_db(db_file, table_name):
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


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


def fetch_data_from_db(db_file, table_name):
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def save_to_db(db_file, table_name, data):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = f'''
    INSERT INTO {table_name} (
        robot_name, schedule, adaptq, stepper, squeeze, preweld_time, preweld_current, cool, slope_up_time,
        slope_up_from, slope_up_to, impulse_time, impulse_cool, weld_time, weld_current, slope_down_time,
        slope_down_from, slope_down_to, hold, full_name, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    for _, row in data.iterrows():
        parameters = (
            row.get('robot_name'),
            row.get('schedule'),
            row.get('adaptq'),
            row.get('stepper'),
            row.get('squeeze'),
            row.get('preweld_time'),
            row.get('preweld_current'),
            row.get('cool'),
            row.get('slope_up_time'),
            row.get('slope_up_from'),
            row.get('slope_up_to'),
            row.get('impulse_time'),
            row.get('impulse_cool'),
            row.get('weld_time'),
            row.get('weld_current'),
            row.get('slope_down_time'),
            row.get('slope_down_from'),
            row.get('slope_down_to'),
            row.get('hold'),
            row.get('full_name'),
            timestamp
        )
        cursor.execute(sql, parameters)

    conn.commit()
    conn.close()


def fetch_latest_record_from_db(db_file, table_name, full_name):
    conn = sqlite3.connect(db_file)
    query = f'''
        SELECT * FROM {table_name}
        WHERE full_name = ?
        ORDER BY timestamp DESC LIMIT 1
    '''
    df = pd.read_sql_query(query, conn, params=(full_name,))
    df = df.drop(columns=['full_name', 'timestamp'])
    conn.close()
    return df.iloc[0] if not df.empty else None


def check_schedule(api_url):
    update_url = api_url.replace('/schedule', '/history/weld/schedule')

    df = fetch_data_from_api(update_url, 'history')
    if df is None:
        return False
    elif df.empty:
        return False
    else:
        return True


def update_db_if_needed(db_file, *, selected_line=None, selected_robot=None):
    table_name = "changelog"

    line_ips_df = read_data_from_db(db_file, 'line_ips')

    ips_df = read_data_from_db(db_file, 'ips')
    line_ips = line_ips_df['ip'].unique()

    if selected_line is not None:
        scan_line = line_ips_df[line_ips_df['line'].str.startswith(selected_line)]
        if selected_robot is not None and selected_line == 'FRM1':
            if int(selected_robot[5]) <= 3:
                line_ips = pd.Series(scan_line['ip'][2])
            elif int(selected_robot[5]) > 3:
                line_ips = pd.Series(scan_line['ip'][3])
        else:
            line_ips = scan_line['ip']

    # Schedule range
    curstom_range = list(map(str, range(1, 256)))


    # uniqe schedules from db
    df = fetch_data_from_db(db_file, 'changelog')
    schedule = sorted(df['schedule'].unique())

    # ------------------------Line loop------------------------------
    for i, l in enumerate(line_ips):

        robot_names = ips_df['robot_name']


        # Matching ip to df
        filtered_line_ip = line_ips_df[line_ips_df['ip'] == l]



        # Changing name from FRM1_1 to FRM1
        if filtered_line_ip['line'].isin(['FRM1_1', 'FRM1_2']).any():
            filtered_line_name = filtered_line_ip['line'].apply(lambda x: x[:4])
        else:
            filtered_line_name = filtered_line_ip['line']

        filtered_robots = [r for r in robot_names if any(r.startswith(line) for line in filtered_line_name)]


        if filtered_line_ip['line'].isin(['FRM1_1']).any():
            filtered_robots = [robot for robot in filtered_robots if int(robot[5]) <= 3]
        elif filtered_line_ip['line'].isin(['FRM1_2']).any():
            filtered_robots = [robot for robot in filtered_robots if int(robot[5]) > 3]

        if selected_robot is not None:
            filtered_robots = pd.Series([selected_robot])

        # -------------------------Robot loop---------------------------
        for r in filtered_robots:
            filtered_ip = ips_df[ips_df['robot_name'] == r]
            selected_ip = filtered_ip['ip'].iloc[0].replace(".", "_")
            robot_id = filtered_ip['robot_id'].iloc[0]

            selected_url = f"http://{l}/4.1.0/timers/{selected_ip}_{robot_id}/schedule/"

            # ---------------------Schedule loop-------------------------
            for s in schedule:
                api_url = selected_url + s

                if not check_schedule(api_url):
                    continue

                api_data = fetch_data_from_api(api_url, 'schedule')

                if api_data is None:
                    continue

                api_df = reformat_df(api_data, r, s)

                if not api_df.empty:
                    api_df['full_name'] = api_df['robot_name'] + str(api_df['schedule'].iloc[0])
                    api_df = api_df.set_index('full_name')

                    records_to_update = []
                    for index, row in api_df.iterrows():
                        lastest_record = fetch_latest_record_from_db(db_file, 'changelog', index)
                        if lastest_record is not None:
                            row = row.reindex(lastest_record.index)
                            if not row.equals(lastest_record):
                                records_to_update.append(row)
                        else:
                            records_to_update.append(row)

                    if records_to_update:
                        records_to_update_df = pd.DataFrame(records_to_update)
                        records_to_update_df['full_name'] = records_to_update_df['robot_name'] + str(
                            records_to_update_df['schedule'].iloc[0])
                        save_to_db(db_file, table_name, records_to_update_df.reset_index())


def format_robot_name(robot_name):
    if pd.isna(robot_name):
        return robot_name
    robot_name = robot_name.replace("-", "").replace("SW", "").replace("MH", "")

    return robot_name


def get_function_data(df, func_code, default_value):
    if isinstance(func_code, str):
        if df['function'].eq(func_code).any():
            return df[df['function'].eq(func_code)]
        else:
            return default_value
    elif isinstance(func_code, list):
        if df['function'].isin(func_code).any():
            return df[df['function'].isin(func_code)]
        else:
            return default_value


def format_value(value, suffix='', default=''):
    if value is not None:
        return str(value) + suffix
    return default


def reformat_df(schedule_df, selected_robot, selected_schedule):
    preweld_list = ['22', '23', '24', '32', '33', '34']
    off = pd.DataFrame(index=range(2))
    off['param_one'] = None
    off['param_two'] = None
    df = pd.DataFrame(index=range(1))

    adaptq = get_function_data(schedule_df, '46', off)
    stepper = get_function_data(schedule_df, '82', off)
    squeeze = get_function_data(schedule_df, '1', off)
    preweld = get_function_data(schedule_df, preweld_list, off)
    cool = get_function_data(schedule_df, '2', off)
    slope = get_function_data(schedule_df,'45', off)
    impuls = get_function_data(schedule_df, '60', off)
    weld = get_function_data(schedule_df, '30', off)
    hold = get_function_data(schedule_df, '3', off)

    # Robot name
    df['robot_name'] = selected_robot

    # Robot schedule
    df['schedule'] = selected_schedule

    # AdaptQ
    df['adaptq'] = format_value(adaptq.iloc[0]['param_one'])

    # Stepper
    df['stepper'] = format_value(stepper.iloc[0]['param_one'])

    # Squeeze
    df['squeeze'] = format_value(squeeze.iloc[0]['param_one'], suffix='ms')

    # Pre weld time
    df['preweld_time'] = format_value(preweld.iloc[0]['param_one'], suffix='ms')

    # Pre weld current
    preweld_current = preweld.iloc[0]['param_two']
    if preweld_current is None:
        df['preweld_current'] = ''
    elif len(str(preweld_current)) == 2:
        df['preweld_current'] = format_value(preweld_current, suffix='%')
    else:
        df['preweld_current'] = format_value(preweld_current, suffix='0A')

    # Cool time
    df['cool'] = format_value(cool.iloc[0]['param_one'], suffix='ms')

    # Slope up
    if not slope.isnull().all().all():
        if int(slope.iloc[0]['param_two']) < int(slope.iloc[0]['param_three']):
            df['slope_up_time'] = format_value(slope.iloc[0]['param_one'], suffix='ms')
            df['slope_up_from'] = format_value(slope.iloc[0]['param_two'], suffix='0A')
            df['slope_up_to'] = format_value(slope.iloc[0]['param_three'], suffix='0A')
        else:
            df['slope_down_time'] = ''
            df['slope_down_from'] = ''
            df['slope_down_to'] = ''
    else:
        df['slope_up_time'] = ''
        df['slope_up_from'] = ''
        df['slope_up_to'] = ''

    # Impulse time
    df['impulse_time'] = format_value(impuls.iloc[0]['param_one'])

    # Impulse cool
    df['impulse_cool'] = format_value(impuls.iloc[0]['param_two'])

    # Weld time
    weld_time = weld.iloc[0]['param_one']
    if weld_time is not None:
        if len(str(weld_time)) == 1:
            df['weld_time'] = format_value(weld_time, suffix='x')
        else:
            df['weld_time'] = format_value(weld_time, suffix='ms')
    else:
        df['weld_time'] = ''

    # Weld current
    df['weld_current'] = format_value(weld.iloc[0]['param_two'], suffix='0A')

    # Slope down
    if schedule_df['function'].eq('45').any():
        if int(slope.iloc[0]['param_two']) > int(slope.iloc[0]['param_three']):
            df['slope_down_time'] = format_value(slope.iloc[0]['param_one'], suffix='ms')
            df['slope_down_from'] = format_value(slope.iloc[0]['param_two'], suffix='0A')
            df['slope_down_to'] = format_value(slope.iloc[0]['param_three'])
        if slope.shape[0] >= 2:
            df['slope_down_time'] = format_value(slope.iloc[1]['param_one'], suffix='ms')
            df['slope_down_from'] = format_value(slope.iloc[1]['param_two'], suffix='0A')
            df['slope_down_to'] = format_value(slope.iloc[1]['param_three'])
        else:
            df['slope_down_time'] = ''
            df['slope_down_from'] = ''
            df['slope_down_to'] = ''
    else:
        df['slope_down_time'] = ''
        df['slope_down_from'] = ''
        df['slope_down_to'] = ''

    # Hold
    df['hold'] = format_value(hold.iloc[0]['param_one'], suffix='ms')

    df.reset_index(drop=True, inplace=True)

    return df


def display_data(db_file, fullname):
    conn = sqlite3.connect(db_file)
    query = '''
    SELECT * FROM changelog WHERE full_name = ?
    '''
    df = pd.read_sql_query(query, conn, params=(fullname,))
    df = df.drop(columns='full_name')
    conn.close()
    df = df.loc[:, (df != '').any(axis=0)]
    if df.empty:
        return st.warning("No data for this schedule")

    return st.write(df.head())

def display_last(db_file):
    conn = sqlite3.connect(db_file)
    query = '''
    SELECT * FROM changelog 
    ORDER BY timestamp DESC
    '''
    df = pd.read_sql_query(query, conn)
    df = df.drop(columns='full_name')
    conn.close()
    df = df.loc[:, (df != '').any(axis=0)]
    if df.empty:
        return st.warning("No data for this schedule")

    return st.write(df.head())



def main():
    st.set_page_config(page_title="Weld History", page_icon="📜", layout="wide")

    # columns
    left_column, gap, middle_column, right_one= st.columns([2, 1, 1, 2])

    db_file = "db/database.db"
    sw_summary = "sw_summary"

    sw_df = read_data_from_db(db_file, sw_summary)
    sw_df = sw_df.dropna(subset=['Line', 'RobotName'])
    uniq_lines = sw_df['Line'].unique()

    changelog = read_data_from_db(db_file, 'changelog')

    # Header
    st.header("Changelog")

    # Left column
    with left_column:
        # Select line
        selected_line = st.selectbox("Line: ", uniq_lines)

        # Filter df based on selected line
        filtered_df = sw_df[sw_df['Line'] == selected_line]

        # Format robot name from filtered df
        robot_names = filtered_df['RobotName'].apply(format_robot_name).unique()

        # Sort robot names
        uniq_robots = sorted(robot_names)

        # Select filtered robot
        selected_robot = st.selectbox("Robot: ", uniq_robots)

        # Select schedule
        schedule_list = list(range(1, 256))
        schedule_list_upgraded = sorted(changelog['schedule'][changelog['robot_name'] == selected_robot].unique(), key=int)
        selected_schedule = st.selectbox("Schedule: ", schedule_list_upgraded)

    # Middle column
    with middle_column:

        scan_choice = st.radio("What do you want to scan👇🏼",
                               ["All", "Line", "Robot"])

    # Right column
    with right_one:

        if scan_choice == "All":
            if st.button("Scan for changes"):
                update_db_if_needed(db_file)
                print("Scanning for changes finished")
                st.balloons()
                st.success("Done")

        if scan_choice == "Line":
            scan_line = st.selectbox("Select line to scan:", uniq_lines)

            if st.button("Scan for changes"):
                update_db_if_needed(db_file, selected_line=scan_line)
                print("Scanning for changes finished")
                st.balloons()
                st.success("Done")

        if scan_choice == "Robot":
            scan_line = st.selectbox("Select line to scan:", uniq_lines)
            robots_for_scan_line = sw_df[sw_df['Line'] == scan_line]
            robots_scan_list = sorted(robots_for_scan_line['RobotName'].apply(format_robot_name).unique())
            scan_robot = st.selectbox("Select robot to scan: ", robots_scan_list)

            if st.button("Scan for changes"):
                update_db_if_needed(db_file, selected_line=scan_line, selected_robot=scan_robot)
                print("Scanning for changes finished")
                st.balloons()
                st.success("Done")



    # Display data
    if selected_schedule:
        full_name = selected_robot + str(selected_schedule)
        display_data(db_file, full_name)


    # Display last changes
    if st.button(f"Last changes"):
        display_last(db_file)




# schedule.every(1).minutes.do(update_db_if_needed, api_url=url, db_file=db_file, table_name=table_name)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)


if __name__ == "__main__":
    main()
