"""
We summarize our data as well as perform data pre-processing and feature 
selection to only include relevant attributes in our learning model.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import OneClassSVM
import time

def summarize():
    """
    Performs data summarization on the dataset.

    We explore and summarize the data to get a “feel” of the data.
    We will conduct data summarization using techniques such as
    scatter plots, box plots, and histograms to visualize and to
    explore attribute characteristics.
    """
    # Load the dataset
    df = pd.read_csv("data_staging/Staged_data.csv")

    # ======================  Boxplot  ======================
    # --- Boxplot for salaries
    title: str = "Salary Range"
    df[["Minimum Salary", "Maximum Salary"]].plot(kind="box", figsize=(10, 6))
    plt.title(title)
    plt.get_current_fig_manager().set_window_title(title)
    plt.show()

    # ======================  Histogram  ======================
    # --- Histogram for experience
    title: str = "Experience Distribution"
    df[["Minimum Experience (years)", "Maximum Experience (years)"]].plot(
        kind="hist", bins=15, alpha=0.5, figsize=(10, 6)
    )
    plt.title(title)
    plt.xlabel("Years of Experience")
    plt.get_current_fig_manager().set_window_title(title)
    plt.show()

    # ======================  Scatter plot  ======================

    # --- Scatter plot for Average Minimum Salary vs Average Minimum Experience

    # Calculate the average minimum salary for each unique value of minimum experience.
    average_salary_by_experience = df.groupby(
        "Minimum Experience (years)", as_index=False
    )["Minimum Salary"].mean()

    # Plot the scatter plot
    title: str = "Average Minimum Salary by Average Minimum Experience"
    plt.figure(figsize=(10, 6))
    plt.scatter(
        average_salary_by_experience["Minimum Experience (years)"],
        average_salary_by_experience["Minimum Salary"],
    )
    plt.xlabel("Minimum Experience (years)")
    plt.ylabel("Average Minimum Salary")
    plt.title(title)
    plt.grid(True)
    plt.get_current_fig_manager().set_window_title(title)
    plt.show()


def transform():
    """
    Performs data transformation to preprocess the data for data mining.

    This step in our data pre-processing stage involves data transformation,
    which includes handling missing values, handling categorical attributes,
    normalization of numeric attributes to ensure all attributes are of equal
    importance during learning, and feature selection to remove potentially
    redundant attributes.
    """
    # Load the dataset
    df = pd.read_csv("data_staging/Staged_data.csv")

    print("===========================  NORMALIZATION  ===========================")
    
    print("[+] Performing Normalization on Salary columns...")
    normalize_salary(df)
    print("==> Normalization on Salary columns completed.\n")
    print("[+] Performing Normalization on Experience columns...")
    normalize_experience(df)
    print("==> Normalization on Experience columns completed.\n")
    
    # Drop the non normalized columns
    print("[+] Dropping non-normalized columns and renaming...\n")
    df.drop(columns=['Minimum Salary', 'Maximum Salary', 'Minimum Experience (years)', 'Maximum Experience (years)'], inplace=True)
    df.rename(columns={'Normalized Minimum Salary': 'Minimum Salary', 'Normalized Maximum Salary': 'Maximum Salary',
    'Normalized Minimum Experience': 'Minimum Experience', 'Normalized Maximum Experience': 'Maximum Experience'}, inplace=True)

    print("===========================  ONE HOT ENCODING  ===========================")
    print("[+] Performing One Hot Encoding on Gender Preference column...")
    df = gender_one_hot_encoding(df)
    print("==> One Hot Encoding on Gender Preference completed.\n")
    print("[+] Performing One Hot Encoding on Work Type column...")
    df = work_type_one_hot_encoding(df)
    print("==> One Hot Encoding on Work Type completed.\n")

    print("===========================  FEATURE SELECTION  ===========================")
    print("[+] Eliminating irrelevant and redundant features...")
    attributes_to_drop = ['Company Ticker', 'Responsibilities', 'Company Sector', 'Specialization']
    print(f"Dropping the following attributes: {attributes_to_drop}\n")
    df.drop(columns=attributes_to_drop, inplace=True)
    
    print("============================     OUTLIERS      ============================")
    print("[+] Identifying the outliers...")
    start_time = time.time() 
    
    find_outliers(df, 'Minimum Salary')
    # find_outliers(df, 'Maximum Salary')
    # find_outliers(df, 'Minimum Experience')
    # find_outliers(df, 'Maximum Experience')

    # Save the transformed csv to a new file
    print("[+] Saving transformed data as a csv file...")
    desired_order = ['Job Id','Minimum Experience',
                 'Maximum Experience','Qualifications',
                 'Minimum Salary', 'Maximum Salary', 'City', 'Country', 'Job City Population',
                 'Contract', 'Full-Time', 'Intern','Part-Time','Temporary', 
                 'Day', 'Month', 'Year', 'Quarter', 'Male', 'Female', 'Both', 'Job Title',
                 'Job Portal', 'Skills', 'Company', 'Company Size',
                 'Company Industry', 'Company HQ City', 'Company HQ Country',
                 'Retirement Plans', 'Stock Options or Equity Grants', 'Parental Leave','Paid Time Off (PTO)',
                 'Flexible Work Arrangements','Health Insurance', 'Life and Disability Insurance',
                 'Employee Assistance Program','Health and Wellness Facilities','Employee Referral Program',
                 'Transportation Benefits','Bonuses and Incentive Programs']
    
    df = df[desired_order]
    
    df.to_csv('data_mining/Transformed_data.csv', index=False)

def find_outliers(df, feature_name):
    print(f"---[+] Identifying the outliers for {feature_name}...")
    start_time = time.time()
    X = df[[feature_name]]

    # Initialize the One-Class SVM model
    nu = 0.1  # Adjust the proportion of outliers to be expected in the data
    model = OneClassSVM(kernel='rbf', nu=nu)

    # Fit the model to the data
    model.fit(X)

    # Predict the labels (inliers: 1, outliers: -1)
    y_pred = model.predict(X)

    end_time = time.time()

    # Calculate time taken in minutes and seconds 
    time_taken = end_time - start_time
    minutes = int(time_taken // 60)
    seconds = int(time_taken % 60)

    print(f"Time taken to calculate outliers for {feature_name}: {minutes} minutes and {seconds} seconds")

    # Visualize the outliers using histogram
    plt.figure(figsize=(8, 6))
    plt.hist(X[feature_name], bins=50, color='#87CEEB', alpha=0.7, label='Data Distribution', density=True)
    plt.hist(X[y_pred == -1][feature_name], bins=50, color='#FF0000', alpha=0.7, label='Outliers', density=True)
    plt.xlabel(feature_name)
    plt.ylabel('Density')
    plt.title('Outlier Detection using One-Class SVM')
    plt.legend()
    plt.show()
    
def normalize_salary(df):
    """
    Normalizes the Salary and Experience columns
    """
    # ======================  Normalization of minimum salary  ====================== 
    
    column_min_salary = 'Minimum Salary'
    data = df[[column_min_salary]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data).round(2)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Minimum Salary" not in df:
        df["Normalized Minimum Salary"] = normalized_data
       
    # ======================  Normalization of maximum salary  ====================== 
     
    column_max_salary = 'Maximum Salary'
    data = df[[column_max_salary]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data).round(2)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Maximum Salary" not in df:
        df["Normalized Maximum Salary"] = normalized_data
    
    # ======================  Display Normalization of Salary  ====================== 
    
    data = df["Minimum Salary"]
    min_value = data.min()
    max_value = data.max()
    print(f"Min value before normalization of Minimum Salary: {min_value}")
    print(f"Max value before normalization of Minimum Salary: {max_value} \n")
    data = df["Maximum Salary"]
    min_value = data.min()
    max_value = data.max()
    print(f"Min value before normalization of Maximum Salary: {min_value}")
    print(f"Max value before normalization Maximum Salary: {max_value} \n")
    
    print("Showing normalization of salary")
    print(df[['Minimum Salary','Normalized Minimum Salary', 'Maximum Salary', 'Normalized Maximum Salary']])
        

def normalize_experience(df):
    """
    Normalizes maximum and minimum years of experience
    """
    # ======================  Normalization of minimum experience  ====================== 
    
    column_min_exp = 'Minimum Experience (years)'
    data = df[[column_min_exp]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data).round(2)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Minimum Experience" not in df:
        df["Normalized Minimum Experience"] = normalized_data
        
    # ======================  Normalization of maximum experience  ====================== 
    
    column_max_exp = 'Maximum Experience (years)'
    data = df[[column_max_exp]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data).round(2)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Maximum Experience" not in df:
        df["Normalized Maximum Experience"] = normalized_data
        
    # ======================  Display Normalization of Experience  ====================== 
    
    data = df["Minimum Experience (years)"]
    min_value = data.min()
    max_value = data.max()
    print(f"Min value before normalization of Minimum Experience: {min_value}")
    print(f"Max value before normalization of Minimum Experience: {max_value}\n")
    data = df["Maximum Experience (years)"]
    min_value = data.min()
    max_value = data.max()
    print(f"Min value before normalization of Maximum Experience: {min_value}")
    print(f"Max value before normalization of Maximum Experience: {max_value}\n")
    
    print("Showing normalization of experience (years)")
    print(df[['Minimum Experience (years)','Normalized Minimum Experience', 'Maximum Experience (years)', 'Normalized Maximum Experience']])


def gender_one_hot_encoding(df):
    """
    Handling Categorical attributes
    Performs One Hot encoding on the Gender Preference column
    """
    if 'Gender Preference' in df.columns and not all(pref in df.columns for pref in ['Male', 'Female', 'Both']):
        # One hot encoding on Gender Preference
        df_encoded = pd.get_dummies(df['Gender Preference'],dtype=np.uint8)

        # Concatenates the encoded columns with the dataframe
        df = pd.concat([df, df_encoded], axis=1)

        # Testing (can remove later)
        print("---[+] Checking if one hot encoding was done properly on Gender Preference column.")
        print(df[['Gender Preference','Male', 'Female', 'Both']])

        # Drop original column if it exists
        if 'Gender Preference' in df.columns:
            print("---[+] Now dropping Gender Preference column...")
            df.drop(columns=['Gender Preference'], inplace=True)

        return df
    else:
        print("--- One Hot Encoding already performed on Gender Preference column.")
        return df

def work_type_one_hot_encoding(df):
    """
    Handling Categorical attributes
    Performs One Hot encoding on the Work Type column
    """
    if 'Work Type' in df.columns and not all(pref in df.columns for pref in ['Contract', 'Full-Time', 'Intern','Part-Time','Temporary']):
        # One hot encoding on Work Type
        df_encoded = pd.get_dummies(df['Work Type'],dtype=np.uint8)

        # Concatenates the encoded columns with the dataframe
        df = pd.concat([df, df_encoded], axis=1)

        # Testing (can remove later)
        print("---[+] Checking if one hot encoding was done properly on Work Type column.")
        print(df[['Work Type','Contract', 'Full-Time', 'Intern','Part-Time','Temporary']])

        # Drop original column if it exists
        if 'Work Type' in df.columns:
            print("---[+] Now dropping Work Type column...")
            df.drop(columns=['Work Type'], inplace=True)

        return df
    else:
        print("--- One Hot Encoding already performed on Work Type column.")
        return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data summarization, data pre-processing and feature selection to only include relevant attributes in the learning model."
    )
    parser.add_argument(
        "--summarize",
        action="store_true",
        help="Summarize the data using boxplots, histograms, scatter plots.",
    )
    parser.add_argument(
        "--transform",
        action="store_true",
        help="Preprocess the data through data transformation and feature selection.",
    )
    args = parser.parse_args()

    if args.summarize:
        summarize()
    elif args.transform:
        transform()
    else:
        summarize()
        transform()
