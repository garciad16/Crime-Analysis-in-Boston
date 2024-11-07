import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sql_operations import create_table, run_queries

# Load CSV
df = pd.read_csv(r'C:\Users\garciad\OneDrive - UHN\Desktop\Tableau Projects\Crime_data_Boston\crime.csv', encoding='ISO-8859-1')

# Display first few rows of the dataset
print(df.head())

# Show column names
print(df.columns)

# Check for missing values
print(df.isnull().sum())

# Example: Fill missing values with a default value
df.fillna(0, inplace=True)

# Convert OCCURRED_ON_D column to datetime
df['OCCURRED_ON_DATE'] = pd.to_datetime(df['OCCURRED_ON_DATE'])

# Summary statistics for numerical columns
print(df.describe())

# Example: Count occurrences of each offense type
offense_counts = df['OFFENSE_DESCRIPTION'].value_counts()
print(offense_counts)

# Example: Number of incidents per day of the week
incidents_per_day = df['DAY_OF_WEEK'].value_counts()
print(incidents_per_day)

# # Limit to top 20 offenses
# top_20_offenses = df['OFFENSE_DESCRIPTION'].value_counts().nlargest(20).index
# plt.figure(figsize=(12,8))  # Increase figure size slightly
# sns.countplot(y='OFFENSE_DESCRIPTION', data=df[df['OFFENSE_DESCRIPTION'].isin(top_20_offenses)], order=top_20_offenses)
# plt.title('Top 20 Crime Count by Offense Description')
# plt.tight_layout()  # Automatically adjust to prevent clipping
# plt.show()
#
# # Plot number of incidents by day of the week
# plt.figure(figsize=(8,5))
# sns.countplot(x='DAY_OF_WEEK', data=df)
# plt.title('Incidents by Day of the Week')
# plt.show()


# Call SQL functions
create_table(df)  # Create the table and insert data
run_queries()     # Run some example queries