import pandas as pd
import numpy as np

for xn in "patient", "provider":

    df1 = pd.read_csv("%s_signals.csv.gz" % xn)
    df1["Location"] = df1.iloc[:, 4:].idxmax(axis=1)
    df1 = df1[["TagId", "Time", "Location"]]
    df1["Time"] = pd.to_datetime(df1.Time)

    df2 = pd.read_csv("../rfid/%s_locations_sm.csv.gz" % xn)
    df2["Time"] = pd.to_datetime(df2.Time)
    df2 = df2[["TagID", "Time", "Room1"]]

    dx = pd.merge(df1, df2, left_on=["TagId", "Time"], right_on=["TagID", "Time"])

    print((dx.Location == dx.Room1).mean())
