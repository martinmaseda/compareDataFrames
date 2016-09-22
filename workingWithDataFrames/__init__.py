"""This is the example module.

Tis module is meant to simplify the search for matches between IDs in DataFrames where the developer does not
know the exact database schema.
"""

__all__ = ['compareDataframes']
__version__ = '0.1'
__author__ = 'Ivan Martin Maseda'

import os
import sys
import pandas as pd
pd.set_option('display.width', 1000)
import warnings


def compareDataFrames(df1, df2, only_check_df1=None, only_check_df2=None):
    """Compare the columns of the two DataFrames to check which columns match best.

    Keyword arguments:
    only_check_df1 -- provide a list of the variables to test from df1. Default will try all variables, not recommended
    only_check_df2 -- provide a list of the variables to test from df2. Default will try all variables, not recommended
    """

    #################################
    # Check values introduced in the parameters are correct.
    # df1 -- Must be a DataFrame
    # df2 -- Must be a DataFrame
    # only_check_df1 -- Must be a list with the correct names of the columns in df1
    # only_check_df2 -- Must be a list with the correct names of the columns in df2
    #################################

    # Check that df1 is a DataFrame
    if (type(df1) != pd.DataFrame) or (type(df2) != pd.DataFrame):
        raise ValueError('Error: Please make sure to introduce a pandas.DataFrame '
                         'object in the first and second parameter')
    # Lets also make sure the DataFrames are not empty to avoid divisions by 0 as much as possible
    if (len(df1) == 0) or (len(df2) == 0):
        raise ValueError('Error: At least one of the two DataFrames introduced has no rows')

    # We first check if the lists of only_check are empty, this is due to the fact that lists are mutable:
    # http://docs.python-guide.org/en/latest/writing/gotchas/
    if only_check_df1 is None:
        only_check_df1_ready = list(df1.keys())
    else:
        # Make sure the column names introduced by the user actually exist and are not duplicated
        only_check_df1_ready = list(set([column for column in only_check_df1 if column in df1.keys()]))
        # if the length of the list hs changed, we must inform he user for him to know that there might be some
        # misspelled column
        if len(only_check_df1_ready) != len(only_check_df1):
            # This is just a warning, the code will continue to run
            warnings.warn("Warning: Some columns have been removed from because they did not exist or were duplicated")
        if len(only_check_df1_ready) == 0:
            # This si an error and if the list of columns to check is empty we will stop execution of this function
            raise ValueError('Error: the final list of columns to check is empty, '
                             'please check the column names are correct')

    # We first check if the lists of only_check are empty, this is due to the fact that lists are mutable:
    # http://docs.python-guide.org/en/latest/writing/gotchas/
    if only_check_df2 is None:
        only_check_df2_ready = list(df2.keys())
    else:
        # Make sure the column names introduced by the user actually exist and are not duplicated
        only_check_df2_ready = list(set([column for column in only_check_df2 if column in df2.keys()]))
        # if the length of the list hs changed, we must inform he user for him to know that there might be some
        # misspelled column
        if len(only_check_df2_ready) != len(only_check_df2):
            # This is just a warning, the code will continue to run
            warnings.warn("Warning: Some columns have been removed from because they did not exist or were duplicated")
        if len(only_check_df2_ready) == 0:
            # This si an error and if the list of columns to check is empty we will stop execution of this function
            raise ValueError('Error: the final list of columns to check is empty, '
                             'please check the column names are correct')
    # Create the dictionary to store all the parameters we want to study
    merge_results = {
        # Name of column from left DataFrame
        "left_col_df1":[],
        # Name of column from right DataFrame
        "left_col_df2":[],
        # Number of rows from the left DataFrame that exist in the right DataFrame (NOTE: this is before merging)
        "matches_on_left_df_before_merge_count":[],
        # Percentage of the rows from the left df that would have a match if we merged (NOTE: this is before merging)
        "matches_on_left_df_before_merge_percent": [],
        # Number of rows in the resulting DataFrame after an INNER join
        "matches_on_left_df_after_merge_count": [],
        # On average, how many rows from the right DataFrame correspond to each row of the left DataFrame
        "matches_on_right_df_for_each_left_match": [],
        # Number of rows from the left DataFrame that will not match any row on the right DataFrame
        "left_not_matches_before_merge_count": [],
        # Percentage of rows from the left DataFrame that will not match any row on the right DataFrame
        "left_not_matches_before_merge_percent": []
    }

    # For every combination of the columns from the left DataFrame with the columns from the right, one by one
    for df1_col in only_check_df1_ready:
        for df2_col in only_check_df2_ready:
            # Introduce the name of the columns being analyzed in this iteration to the list of the dictionary
            # NOTE: Remember dictionaries do not maintain the order ot the keys, but the list inside them do.
            merge_results["left_col_df1"].append(df1_col)
            merge_results["left_col_df2"].append(df2_col)
            # Count the values from Left DataFrame that exist in the right DataFrame
            matches_before_count = sum([True for value in list(df1[df1_col]) if value in list(df2[df2_col])])
            # Calculate the percentage out of the total for the previous count
            matches_before_percent = float(matches_before_count)/len(df1)
            # Perform the merge to calculate the number of items in the resulting DataFrame.
            # NOTE: Remember that inner will replicate the row on the left DataFrame
            # if the same ID appears in the right DataFrame
            # DF1   DF2                         DF1
            #   A     A     Will result into ->   A
            #   B     A                           A
            #   C     B                           B
            matches_after_count = len(pd.merge(pd.DataFrame(df1[df1_col]), pd.DataFrame(df2[df2_col]),
                                           how='inner', left_on=[df1_col], right_on=[df2_col]))
            # Percentage of rows from the left DataFrame that will not match any row on the right DataFrame
            matches_after_targets_per_match = float(matches_after_count) / matches_before_count
            # Number of rows from the left DataFrame that will not match any row on the right DataFrame
            not_matches_before_merge_count = len(df1) - matches_before_count
            # Percentage of rows from the left DataFrame that will not match any row on the right DataFrame
            not_matches_before_merge_percent = not_matches_before_merge_count / len(df1)

            # Append the final values to the corresponding list int eh dictionary
            merge_results["matches_on_left_df_before_merge_count"].append(matches_before_count)
            merge_results["matches_on_left_df_before_merge_percent"].append(matches_before_percent)
            merge_results["matches_on_left_df_after_merge_count"].append(matches_after_count)
            merge_results["matches_on_right_df_for_each_left_match"].append(matches_after_targets_per_match)
            merge_results["left_not_matches_before_merge_count"].append(not_matches_before_merge_count)
            merge_results["left_not_matches_before_merge_percent"].append(not_matches_before_merge_percent)
    # End of FOR
    # Define here the order in which we want to display the results,
    # remember that the dictionary might change the order.
    order_of_columns = ['left_col_df1',
                        'left_col_df2',
                        'matches_on_left_df_before_merge_percent',
                        'matches_on_right_df_for_each_left_match',
                        'matches_on_left_df_before_merge_count',
                        'matches_on_left_df_after_merge_count',
                        'left_not_matches_before_merge_count',
                        'left_not_matches_before_merge_percent']
    # Apply the order of the columns and sor the rows by the percentage of matches and
    # the average number of right rows fro every left row
    return pd.DataFrame(merge_results)[order_of_columns].sort_values(["matches_on_left_df_before_merge_percent",
                                                               "matches_on_right_df_for_each_left_match"],
                                                              ascending = [0, 1])

####################################
########### TEST SECTION ###########
####################################

test_dict_for_dataframe_1 = {"the_id_1": pd.Series([1,1,2,3,4,5]), "not_the_id_1": pd.Series([4,5,62,7,48,9])}
test_dict_for_dataframe_2 = {"the_id_2": pd.Series([1,2,3,4,5,5]), "not_the_id_2": pd.Series([4,5,6,74,8,100])}

test_df_for_dataframe_1 = pd.DataFrame(test_dict_for_dataframe_1)
test_df_for_dataframe_2 = pd.DataFrame(test_dict_for_dataframe_2)

print(compareDataFrames(test_df_for_dataframe_1, test_df_for_dataframe_2, only_check_df1=["not_the_id_1"]))
print(compareDataFrames(test_df_for_dataframe_1, test_df_for_dataframe_2))


