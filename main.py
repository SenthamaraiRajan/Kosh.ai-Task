import fitz  # PyMuPDF
import pandas as pd
import re
import sqlite3

# Open the PDF
doc = fitz.open("data/Test PDF.pdf")

# Initialize an empty string to store text
text = ""

# Extract text from each page
for page in doc:
    text += page.get_text()

# Close the document
doc.close()

# Define a pattern that matches the rows of data in the text
# Adjust the pattern as necessary based on the structure of the text
pattern = r"\n(\d{8})\s*(\d{9})\s*(\d{1,2}/\d{1,2}/\d{4})\s*([A-Z].*)\s*( ?|[A-Z-]?[a-z-]*(?: +[A-Z-][a-z-]*[A-Z]?[a-z]*)*)\s*([A-Z-]+(?:\s+[A-Z-]+)*)\s*([A-Z].*)\s*([\d,]+.\d{2})\s*(\d.\d{2})\s*([\d,]+.\d{2})\s*([\d,]+.\d{2})"


# Find all matches in the text
matches = re.findall(pattern,str(text))

    
df = pd.DataFrame(matches, columns=[
    'App ID', 'Xref', 'Settlement Date', 'Broker', 'Sub Broker', 'Borrower Name', 'Description', 'Total Loan Amount', 'Comm Rate', 'Upfront', 'Upfront Incl GST'
])

# Convert numeric columns from strings to appropriate types
numeric_columns = ['Total Loan Amount', 'Comm Rate', 'Upfront', 'Upfront Incl GST']
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors='coerce')


# Connect to the SQLite database
conn = sqlite3.connect('data/loan_data.db')

# SQL command to create the 'loan_info' table with the specified schema and uniqueness constraint
create_table_sql = """
CREATE TABLE IF NOT EXISTS loan_info (
    app_id TEXT,
    xref TEXT,
    date TEXT,
    broker TEXT,
    sub_broker TEXT,
    borrower_name TEXT,
    description TEXT,
    total_loan_amount REAL,
    comm_rate REAL,
    upfront REAL,
    upfront_incl_gst REAL,
    UNIQUE(xref, total_loan_amount) ON CONFLICT IGNORE
);
"""

# Execute the SQL command
cur = conn.cursor()
cur.execute(create_table_sql)

# Commit the changes and close the connection
conn.commit()


# Now, let's replace the existing table with the deduplicated dataframe to ensure our datastore is updated
df.to_sql('loan_info', conn, if_exists='replace', index=False)


# 1. Calculate the total loan amount for the available transactions (as a proxy for a specific time period)
total_loan_amount_query = "SELECT SUM(`Total Loan Amount`) AS `Total Loan Amount` FROM loan_info;"
total_loan_amount = pd.read_sql(total_loan_amount_query, conn)

# 2. Calculate the highest loan amount given by a broker
highest_loan_by_broker_query = """
SELECT Broker, MAX(`Total Loan Amount`) AS `Highest Loan Amount`
FROM loan_info
GROUP BY Broker
ORDER BY `Highest Loan Amount` DESC
LIMIT 1;
"""
highest_loan_by_broker = pd.read_sql(highest_loan_by_broker_query, conn)



print(total_loan_amount)
print(highest_loan_by_broker)

# Retrieving the data from 'loan_info' table
df = pd.read_sql('SELECT * FROM loan_info', conn)

# Closing the connection
conn.close()
# print(df.columns)
# Converting 'date' column to datetime format for easier manipulation
df['Settlement Date'] = pd.to_datetime(df['Settlement Date'],format='%d/%m/%Y')

# 1. Generate a report for the broker, sorting loan amounts in descending order for daily, weekly, and monthly periods
# For weekly and monthly reports, we'll group by the start of the week/month for clarity
df['week_start'] = df['Settlement Date'] - pd.to_timedelta(df['Settlement Date'].dt.dayofweek, unit='d')  # Week starts on Monday
df['month'] = df['Settlement Date'].dt.to_period('M')

# Daily Report
daily_report = df.groupby(['Settlement Date', 'Broker'])['Total Loan Amount'].sum().reset_index().sort_values(by=['Settlement Date', 'Total Loan Amount'], ascending=[True, False])
daily_report.to_csv('data/daily_broker_report.csv', index=False)

# Weekly Report
weekly_report = df.groupby(['week_start', 'Broker'])['Total Loan Amount'].sum().reset_index().sort_values(by=['week_start', 'Total Loan Amount'], ascending=[True, False])
weekly_report.to_csv('data/weekly_broker_report.csv', index=False)

# Monthly Report
monthly_report = df.groupby(['month', 'Broker'])['Total Loan Amount'].sum().reset_index().sort_values(by=['month', 'Total Loan Amount'], ascending=[True, False])
monthly_report.to_csv('data/monthly_broker_report.csv', index=False)

# 2. Report of the total loan amount grouped by date
date_grouped_report = df.groupby('Settlement Date')['Total Loan Amount'].sum().reset_index()
date_grouped_report.to_csv('data/total_loan_by_date_report.csv', index=False)

# 3. Define tier level of each transaction
def classify_tier(row):
    if row['Total Loan Amount'] > 100000:
        return 'Tier 1'
    elif row['Total Loan Amount'] > 50000:
        return 'Tier 2'
    elif row['Total Loan Amount'] > 10000:
        return 'Tier 3'
    else:
        return 'Below Tier 3'

df['tier'] = df.apply(classify_tier, axis=1)

# 4. Generate a report of the number of loans under each tier group by date
tier_grouped_report = df.groupby(['Settlement Date', 'tier']).size().reset_index(name='loan_count')
tier_grouped_report.to_csv('data/loan_count_by_tier_and_date_report.csv', index=False)



