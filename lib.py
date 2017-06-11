import pandas as pd

def row_count_distribution(df, col_name):
    df_=df[[col_name]].copy()
    df_["count"]=1
    df_=df_.groupby(col_name).sum().sort_values(by="count", ascending=False)
    return df_.reset_index()

def count_of_B_per_A(df, colA, colB):
    df["count"]=0
    x=df[[colA, colB, "count"]].groupby((colA, colB)).sum().reset_index()
    x["count"]=1
    y=x.groupby(colA)[["count"]].sum().sort_values(by="count", ascending=False)
    del df["count"]
    return y.reset_index()

