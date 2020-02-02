import pandas as pd


def prep_data(df):
    """Five game rolling average stats"""
    rolling = (
        df.groupby(["player_id", "position"])[["goals", "assists", "shots", "ice_time"]]
        .rolling(5)
        .mean()
        .reset_index()
        .rename(columns={"level_2": "index"})
        .set_index("index")
        .dropna(subset=["goals"])[["position", "goals", "assists", "shots", "ice_time"]]
    )
    return rolling
