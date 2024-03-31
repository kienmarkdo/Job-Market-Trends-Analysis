"""
We summarize our data as well as perform data pre-processing and feature 
selection to only include relevant attributes in our learning model.
"""

import pandas as pd
import matplotlib.pyplot as plt
import argparse


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
    pass


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
