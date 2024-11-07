import sqlite3
import pandas as pd

def create_table(df):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('crime_data.db')
        cursor = conn.cursor()

        # Create table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            INCIDENT_NUM TEXT,
            OFFENSE_CODE INT,
            OFFENSE_CODE_GROUP TEXT,
            OFFENSE_DESCRIPTION TEXT,
            DISTRICT TEXT,
            REPORTING_AREA INT,
            OCCURRED_ON_DATE TEXT,
            YEAR INT,
            MONTH INT,
            DAY_OF_WEEK TEXT,
            LONG REAL,
            LAT REAL
        );
        ''')
        conn.commit()

        # Insert data into the table
        df.to_sql('incidents', conn, if_exists='replace', index=False)
        conn.commit()

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def run_queries():
    try:
        # Connect to the database
        conn = sqlite3.connect('crime_data.db')
        cursor = conn.cursor()

        # Query 1: Offense counts
        print("Query 1: Offense counts")
        cursor.execute(
            "SELECT OFFENSE_DESCRIPTION, COUNT(*) FROM incidents GROUP BY OFFENSE_DESCRIPTION ORDER BY COUNT(*) DESC")
        results = cursor.fetchall()

        # Convert results to DataFrame and export to CSV
        df_offense_counts = pd.DataFrame(results, columns=['OffenseDescription', 'Count'])
        df_offense_counts.to_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\offense_counts.csv', index=False)

        print(df_offense_counts)

        # Query 2: Streets with more than 10 average incidents per month (with lat/long)
        print("\nQuery 2: Streets with more than 10 average incidents per month (with lat/long)")
        cursor.execute('''
            SELECT STREET, LAT, LONG, AVG(monthly_incidents) AS avg_incidents_per_month
            FROM (
                SELECT STREET, LAT, LONG, MONTH, COUNT(*) AS monthly_incidents
                FROM incidents
                GROUP BY STREET, LAT, LONG, MONTH
            ) AS monthly_totals
            GROUP BY STREET, LAT, LONG
            HAVING avg_incidents_per_month > 10
            ORDER BY avg_incidents_per_month DESC
        ''')

        avg_incidents_per_month = cursor.fetchall()

        # Convert results to DataFrame and export to CSV
        df_avg_incidents_per_month = pd.DataFrame(avg_incidents_per_month,
                                                  columns=['Street', 'Lat', 'Long', 'AvgIncidentsPerMonth'])
        df_avg_incidents_per_month.to_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\avg_incidents_per_month.csv', index=False)

        print(df_avg_incidents_per_month)

        # Query 3: Number of incidents by month and weekday
        print("\nQuery 3: Number of incidents by month and weekday")
        cursor.execute('''
            SELECT MONTH, DAY_OF_WEEK, COUNT(*) AS total_incidents
            FROM incidents
            GROUP BY MONTH, DAY_OF_WEEK
            ORDER BY MONTH, 
            CASE 
                WHEN DAY_OF_WEEK = 'Sunday' THEN 1
                WHEN DAY_OF_WEEK = 'Monday' THEN 2
                WHEN DAY_OF_WEEK = 'Tuesday' THEN 3
                WHEN DAY_OF_WEEK = 'Wednesday' THEN 4
                WHEN DAY_OF_WEEK = 'Thursday' THEN 5
                WHEN DAY_OF_WEEK = 'Friday' THEN 6
                WHEN DAY_OF_WEEK = 'Saturday' THEN 7
            END
        ''')

        monthly_weekday_totals = cursor.fetchall()

        # Convert results to DataFrame and export to CSV
        df_monthly_weekday = pd.DataFrame(monthly_weekday_totals, columns=['Month', 'DayOfWeek', 'TotalIncidents'])
        df_monthly_weekday.to_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\total_incidents_by_month_weekday.csv', index=False)

        print(df_monthly_weekday)

        # Query 4: Total incidents per month
        print("\nQuery 4: Total incidents per month")
        cursor.execute('''
            SELECT MONTH, COUNT(*) AS total_incidents
            FROM incidents
            GROUP BY MONTH
            ORDER BY MONTH
        ''')

        monthly_totals = cursor.fetchall()

        # Convert results to DataFrame and export to CSV
        df_monthly_totals = pd.DataFrame(monthly_totals, columns=['Month', 'TotalIncidents'])

        print(df_monthly_totals)

        # Calculate percentage change month-over-month
        df_monthly_totals['PercentageChange'] = df_monthly_totals['TotalIncidents'].pct_change() * 100

        # Fill any NaN values (for the first month since there's no previous month to compare to)
        df_monthly_totals.fillna(0, inplace=True)

        # Print and export the monthly trend with percentage changes
        df_monthly_totals.to_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\monthly_percentage_change.csv', index=False)
        print(df_monthly_totals)

        # Query 5: Total number of incidents by year
        print("\nQuery 5: Total number of incidents by year")
        cursor.execute('''
            SELECT YEAR, COUNT(*) AS total_incidents
            FROM incidents
            GROUP BY YEAR
            ORDER BY YEAR
        ''')

        yearly_totals = cursor.fetchall()

        # Convert results to DataFrame and export to CSV
        df_yearly_totals = pd.DataFrame(yearly_totals, columns=['Year', 'TotalIncidents'])

        print(df_yearly_totals)

        # Calculate the Year-over-Year percentage change
        df_yearly_totals['YoYPercentageChange'] = df_yearly_totals['TotalIncidents'].pct_change() * 100

        # Fill NaN values (for the first year since there's no previous year to compare)
        df_yearly_totals.fillna(0, inplace=True)

        # Export the yearly data with year-over-year percentage change
        df_yearly_totals.to_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\yearly_percentage_change.csv', index=False)
        print(df_yearly_totals)


        # Query 6: Group incidents by time range
        print("\nQuery 6: Crime incidents by time range")
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 0 AND 2 THEN '12:00 AM - 2:59 AM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 3 AND 5 THEN '3:00 AM - 5:59 AM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 6 AND 8 THEN '6:00 AM - 8:59 AM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 9 AND 11 THEN '9:00 AM - 11:59 AM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 12 AND 14 THEN '12:00 PM - 2:59 PM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 15 AND 17 THEN '3:00 PM - 5:59 PM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 18 AND 20 THEN '6:00 PM - 8:59 PM'
                    WHEN CAST(strftime('%H', OCCURRED_ON_DATE) AS INT) BETWEEN 21 AND 23 THEN '9:00 PM - 11:59 PM'
                END AS time_range,
                COUNT(*) AS total_incidents
            FROM incidents
            GROUP BY time_range
            ORDER BY total_incidents DESC
        ''')

        time_range_totals = cursor.fetchall()

        # Convert SQL query results to a pandas DataFrame
        df_time_range = pd.DataFrame(time_range_totals, columns=['TimeRange', 'TotalIncidents'])

        # Export to csv
        df_time_range.to_csv(
            r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\crime_by_time_range.csv',
            index=False)
        # Print the time range data
        print(df_time_range)

        # Query 7: Get both the most dangerous and least dangerous crime times
        print("\nQuery: Most and Least Dangerous Crime Times")
        cursor.execute('''
            SELECT crime_time, total_incidents, 'Most Dangerous' AS category FROM (
                SELECT strftime('%H:%M', OCCURRED_ON_DATE) AS crime_time, COUNT(*) AS total_incidents
                FROM incidents
                GROUP BY crime_time
                ORDER BY total_incidents DESC
                LIMIT 1
            )
            UNION ALL
            SELECT crime_time, total_incidents, 'Least Dangerous' AS category FROM (
                SELECT strftime('%H:%M', OCCURRED_ON_DATE) AS crime_time, COUNT(*) AS total_incidents
                FROM incidents
                GROUP BY crime_time
                ORDER BY total_incidents ASC
                LIMIT 1
            )
        ''')

        # Fetch the most and least dangerous crime times
        dangerous_times = cursor.fetchall()

        # Close the connection
        conn.close()

        # Process the most and least dangerous times into two rows
        most_dangerous_time = dangerous_times[0]
        least_dangerous_time = dangerous_times[1]

        # Create a pandas DataFrame with two columns: Most Dangerous and Least Dangerous
        df_dangerous_times = pd.DataFrame({
            'MostDangerousTime': [most_dangerous_time[0]],  # Already formatted as HH:MM from the SQL query
            'MostDangerousIncidents': [most_dangerous_time[1]],
            'LeastDangerousTime': [least_dangerous_time[0]],  # Already formatted as HH:MM from the SQL query
            'LeastDangerousIncidents': [least_dangerous_time[1]]
        })

        # Print the DataFrame
        print(df_dangerous_times)

        # Export to CSV
        df_dangerous_times.to_csv(
            r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\Queries\dangerous_crime_times.csv',
            index=False)

        # Close the connection
        conn.close()


    except sqlite3.Error as e:
        print(f"An error occurred while querying: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()