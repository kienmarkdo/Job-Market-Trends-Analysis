"""
We summarize our data as well as perform data pre-processing and feature 
selection to only include relevant attributes in our learning model.
"""

import pandas as pd
import matplotlib.pyplot as plt
import argparse
from sklearn.preprocessing import MinMaxScaler


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
    
    normalize_salary(df)
    normalize_experience(df)
    
    # print statements
    print("\n5 rows of the Minimum Salary column:")
    print(df["Minimum Salary"].head(5))
    print("\n5 rows of the Normalized Minimum Salary column:")
    print(df["Normalized Minimum Salary"].head(5))
    
    print("\n5 rows of the Maximum Salary column:")
    print(df["Maximum Salary"].head(5))
    print("\n5 rows of the Normalized Maximum Salary column:")
    print(df["Normalized Maximum Salary"].head(5))
    
    print("\n5 rows of the Minimum Experience column:")
    print(df["Minimum Experience (years)"].head(5))
    print("\n5 rows of the Normalized Minimum Experience column:")
    print(df["Normalized Minimum Experience"].head(5))
    
    print("\n5 rows of the Maximum Experience column:")
    print(df["Maximum Experience (years)"].head(5))
    print("\n5 rows of the Normalized Maximum Experience column:")
    print(df["Normalized Maximum Experience"].head(5))

    # Save the transformed csv to a new file
    # df.to_csv('transformed_data.csv', index=False)
    
    

def normalize_salary(df):
    """
    Normalizes the Salary and Experience columns
    """
    # ======================  Normalization of minimum salary  ====================== 
    column_min_salary = 'Minimum Salary'  # Replace 'column_name' with the name of your column
    data = df[[column_min_salary]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Minimum Salary" not in df:
        df["Normalized Minimum Salary"] = normalized_data
       
    # ======================  Normalization of maximum salary  ====================== 
     
    column_max_salary = 'Maximum Salary'
    data = df[[column_max_salary]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Maximum Salary" not in df:
        df["Normalized Maximum Salary"] = normalized_data
        
    return df

def normalize_experience(df):
    """
    Normalizes maximum and minimum years of experience
    """
    # ======================  Normalization of minimum experience  ====================== 
    
    column_min_exp = 'Minimum Experience (years)'
    data = df[[column_min_exp]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Minimum Experience" not in df:
        df["Normalized Minimum Experience"] = normalized_data
        
    # ======================  Normalization of maximum experience  ====================== 
    
    column_max_exp = 'Maximum Experience (years)'  # Replace 'column_name' with the name of your column
    data = df[[column_max_exp]]
    
    # Normalize
    scaler = MinMaxScaler()
    normalized_data = scaler.fit_transform(data)
    
    # Add the normalized column to the df if it doesn't exist yet
    if "Normalized Maximum Experience" not in df:
        df["Normalized Maximum Experience"] = normalized_data
        
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
