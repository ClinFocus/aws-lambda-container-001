
import json
import boto3
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime, timedelta

# Define environment variables
db_username = 'admin'
db_password = "GiLL3gER098##"
db_host = "datasetsrds.cf3f9ceipnbs.us-east-1.rds.amazonaws.com"
db_name = "elu_quicksight_dataset"
db_port = '3306'  # Change the port if necessary

# Define the specific file name you want to trigger the Lambda for
specific_file_name = 'Hematology19OCT.csv'


def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    # Replace 'test' with your S3 bucket name
    s3_bucket = 'rdssftp'

    for record in event['Records']:
        # Get the S3 bucket and key from the S3 event
        s3_key = record['s3']['object']['key']
        print(f"S3 Bucket: {s3_bucket}, S3 Key: {s3_key}")
        # Check if the file name matches the specific file name
        if s3_key == specific_file_name:
            try:
                # Read the specific CSV file from S3 into a DataFrame
                print("Before error")
                response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                print("After Before error")
                csv_file = response['Body']
                df = pd.read_csv(csv_file)

                # Rest of your code (data transformations) goes here


                # Columns to select from the CSV
                columns_to_select = [
                    'Subject',
                    'FolderName',
                    'LB1DAT_RAW',
                    'LB1HGB',
                    'LB1HGB_UN',
                    'LB1HCT',
                    'LB1HCT_UN',
                    'LB1PLAT',
                    'LB1PLAT_UN',
                    'LB1RBC',
                    'LB1RBC_UN',
                    'LB1WBC',
                    'LB1WBC_UN',
                    'LB1MCV',
                    'LB1MCV_UN',
                    'LB1MCH',
                    'LB1MCH_UN',
                    'LB1RDW',
                    'LB1RDW_UN',
                    'LB1RC',
                    'LB1RC_UN',
                    'LB1NC',
                    'LB1NC_UN',
                    'LB1LC',
                    'LB1LC_UN',
                    'LB1MC',
                    'LB1MC_UN',
                    'LB1EC',
                    'LB1EC_UN',
                    'LB1BC',
                    'LB1BC_UN',
                    'LB1NEUT',
                    'LB1NEUT_UN',
                    'LB1LYM',
                    'LB1LYM_UN',
                    'LB1MONO',
                    'LB1MONO_UN',
                    'LB1EOS',
                    'LB1EOS_UN',
                    'LB1BASO',
                    'LB1BASO_UN'
                ]

                # Filter rows where LB1DAT_RAW is not null
                df = df.dropna(subset=['LB1DAT_RAW'])

                # Define a mapping for renaming 'FolderName'
                foldername_mapping = {
                    'Screening': 'SCR',
                    'Cycle 1 Day 1': 'C1D1',
                    'Cycle 1 Day 8': 'C1D8',
                    'Cycle 1 Day 15': 'C1D15',
                    'Cycle 2 Day 1': 'C2D1',
                    'End of Treatment': 'EOT',
                    'Unscheduled': 'UNSCH',
                    'Cycle 2 Day 8': 'C2D8',
                    'Cycle 2 Day 15': 'C2D15',
                    'Cycle 3 Day 1': 'C3D1',
                    'Cycle 3 Day 8': 'C3D8',
                    'Cycle 3 Day 15': 'C3D15',
                    'Cycle 4 Day 1': 'C4D1',
                    'Cycle 4 Day 8': 'C4D8',
                    'Cycle 4 Day 15': 'C4D15',
                    'Cycle 5 Day 1': 'C5D1',
                    'Cycle 5 Day 8': 'C5D8',
                    'Cycle 5 Day 15': 'C5D15',
                    'Cycle 6 Day 1': 'C6D1',
                    'Cycle 6 Day 8': 'C6D8',
                    'Cycle 6 Day 15': 'C6D15',
                    'Cycle 7 Day 1': 'C7D1',
                    'Cycle 7 Day 8': 'C7D8',
                    'Cycle 7 Day 15': 'C7D15',
                    'Cycle 8 Day 1': 'C8D1',
                    'Cycle 8 Day 8': 'C8D8',
                    'Cycle 8 Day 15': 'C8D15',
                    'Cycle 9 Day 1': 'C9D1',
                    'Cycle 9 Day 8': 'C9D8',
                    'Cycle 9 Day 15': 'C9D15',
                    'Cycle 10 Day 1': 'C10D1',
                    'Cycle 10 Day 8': 'C10D8',
                    'Cycle 10 Day 15': 'C10D15',
                    'Cycle 11 Day 1': 'C11D1',
                    'Cycle 11 Day 8': 'C11D8',
                    'Cycle 11 Day 15': 'C11D15',
                    'Cycle 12 Day 1': 'C12D1',
                    'Cycle 12 Day 8': 'C12D8',
                    'Unscheduled': 'UNSCH',
                    'Cycle 12 Day 15': 'C12D15',
                    'Cycle 13 Day 1': 'C13D1',
                    'Cycle 13 Day 15': 'C13D15',
                    'Cycle 14 Day 1': 'C14D1',
                    'Cycle 14 Day 15': 'C14D15',
                }

                # Rename the 'FolderName' column based on the mapping
                df['FolderName'] = df['FolderName'].map(foldername_mapping).fillna(df['FolderName'])


                # Create a connection to the RDS database
                engine = create_engine(f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}")
                eligibility_table_name = 'ELIG_table'
                # Read the "Eligibility" table into a DataFrame
                eligibility_df = pd.read_sql_table(eligibility_table_name, con=engine)

                # Apply transformations to the "IE.PART" column
                eligibility_df['PART'] = eligibility_df['PART'].replace('', 'Part 2')
                eligibility_df['PART'] = eligibility_df['PART'].replace('Part 2 Stage 1', 'Part 2')

                # Apply transformations to the "IE.IECOHORT1A" column
                eligibility_df['IECOHORT1A'] = eligibility_df['IECOHORT1A'].replace('', 'No Cohort')

                # Merge the DataFrames based on the "Subject" column
                result_df = pd.merge(df, eligibility_df, on='Subject', how='left')

                # Replace NaN values with "Part 2" in the "IE.PART" column
                result_df['PART'] = result_df['PART'].fillna('Part 2')

                # Replace NaN values with "No Cohort" in the "IE.IECOHORT1A" column
                result_df['IECOHORT1A'] = result_df['IECOHORT1A'].fillna('No Cohort')


                # calculate days code


                # Convert the 'LB1DAT_RAW' column to datetime objects
                result_df['LB1DAT_RAW'] = pd.to_datetime(result_df['LB1DAT_RAW'], errors='coerce')  # 'coerce' to handle invalid date strings

                # Remove rows with missing 'LB1DAT_RAW' values
                result_df = result_df.dropna(subset=['LB1DAT_RAW'])

                # Function to calculate the number of days from a given reference date for each subject


                def calculate_days_from_reference(group, reference_date):
                    # Sort by date
                    group = group.sort_values(by='LB1DAT_RAW')

                    # Calculate days from the reference date
                    group['Days'] = (group['LB1DAT_RAW'] - reference_date).dt.days

                    # Replace negative days with NaN
                    group['Days'] = group['Days'].apply(lambda x: np.nan if x < 0 else x)

                    return group

                # Define the reference date for C1D8 (assuming 8 days before C1D8 is C1D1)
                reference_date_for_C1D8 = 8

                # Initialize an empty DataFrame to store the updated data
                updated_result_df = pd.DataFrame(columns=result_df.columns)

                # Apply the following logic for each subject
                for subject, subject_data in result_df.groupby('Subject'):
                    if 'C1D1' in subject_data['FolderName'].values:
                        # If 'C1D1' is present, calculate days from 'C1D1'
                        C1D1_date = subject_data.loc[subject_data['FolderName'] == 'C1D1', 'LB1DAT_RAW'].min()
                        subject_data = calculate_days_from_reference(subject_data, C1D1_date)
                    else:
                        # If 'C1D1' is not present, calculate 'C1D1' from 'C1D8'
                        C1D8_date = subject_data.loc[subject_data['FolderName'] == 'C1D8', 'LB1DAT_RAW'].min()
                        C1D1_date = C1D8_date - pd.Timedelta(days=reference_date_for_C1D8)
                        
                        # Add 'C1D1' data for this subject
                        C1D1_data = pd.DataFrame({'Subject': [subject], 'FolderName': ['C1D1'], 'LB1DAT_RAW': [C1D1_date], 'Days': [0]})
                        subject_data = pd.concat([subject_data, C1D1_data], ignore_index=True)

                        # Calculate days from 'C1D1'
                        subject_data = calculate_days_from_reference(subject_data, C1D1_date)

                    # Append the updated subject data to the result DataFrame
                    updated_result_df = pd.concat([updated_result_df, subject_data], ignore_index=True)



                # end of calculate days code

                # start CycleDays code

                # Convert 'LB1DAT_RAW' column to datetime
                updated_result_df['LB1DAT_RAW'] = pd.to_datetime(updated_result_df['LB1DAT_RAW'], format='%d %b %Y')

                # Sort the DataFrame by 'Subject' and 'LB1DAT_RAW' to ensure dates are in ascending order
                updated_result_df = updated_result_df.sort_values(by=['Subject', 'LB1DAT_RAW'])

                # Create a dictionary to store the cycle start dates for each subject
                cycle_start_dates = {}

                # Function to calculate the cycle days for UNSCH
                def calculate_cycle_days(row):
                    if row['FolderName'].startswith('UNSCH'):
                        subject = row['Subject']
                        unsch_date = row['LB1DAT_RAW']
                        last_cycle_start = None
                        last_cycle_number = None

                        for _, cycle_row in updated_result_df.iterrows():
                            if cycle_row['Subject'] == subject and cycle_row['FolderName'].endswith('D1'):
                                cycle_start_date = cycle_row['LB1DAT_RAW']
                                if cycle_start_date <= unsch_date:
                                    last_cycle_start = cycle_start_date
                                    last_cycle_number = int(cycle_row['FolderName'].split('C')[1].split('D1')[0])
                                else:
                                    break

                        if last_cycle_start:
                            days_from_last_cycle_start = (unsch_date - last_cycle_start).days
                
                            return f'UNSCH{last_cycle_number}D{days_from_last_cycle_start}'

                    return row['FolderName']

                # Apply the function to calculate 'CycleDays'
                updated_result_df['CycleDays'] = updated_result_df.apply(calculate_cycle_days, axis=1)

                # Extract numbers from 'CycleDays' and format them as '04' and '06'
                updated_result_df['FolderName_Order'] = updated_result_df['CycleDays'].str.extract(r'(C\d+D\d+|UNSCH\d+D\d+)')
                updated_result_df['FolderName_Order'] = updated_result_df['FolderName_Order'].str.extract(r'(\d+)D(\d+)').apply(
                    lambda x: f"{int(x[0]):02d}{int(x[1]):02d}" if not x.isna().any() else np.nan,
                    axis=1
                )

                # Update 'FolderName_Order' column for "SCR" and "EOT"
                updated_result_df.loc[updated_result_df['CycleDays'].str.contains('SCR', na=False), 'FolderName_Order'] = 0
                updated_result_df.loc[updated_result_df['CycleDays'].str.contains('EOT', na=False), 'FolderName_Order'] = 8888


                # ?end of CycleDays code

                # END OF TRANSFORMATIONS
                # Save the result to the RDS database
                table_name = 'HematologyNew_table'
                df.to_sql(table_name, con=engine, if_exists='replace', index=False)
                engine.dispose()

                print(f"Data from specific CSV file '{specific_file_name}' saved to RDS table '{table_name}'")

            except Exception as e:
                print(f"An error occurred: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps("Function executed successfully")
    }