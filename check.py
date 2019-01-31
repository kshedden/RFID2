import pandas as pd
import numpy as np

for xn in "patient", "provider":

    # Find the room with the strongest quantitative signal
    df1 = pd.read_csv("%s_signals.csv.gz" % xn)
    df1["Location"] = df1.iloc[:, 5:].idxmax(axis=1)
    vars = ["TagId", "Time", "Location"]
    if xn == "patient":
        vars.extend(["CSN", "ClarityStart", "ClarityEnd"])
    else:
        vars.append("UMid")
    df1 = df1[vars]
    df1["Time"] = pd.to_datetime(df1.Time)

    df2 = pd.read_csv("../rfid/%s_locations_sm.csv.gz" % xn)
    df2["Time"] = pd.to_datetime(df2.Time)
    df2 = df2[["TagID", "CSN", "UMid", "Time", "Room1"]]

    if xn == "patient":
        dx = pd.merge(df1, df2, left_on=["CSN", "Time"], right_on=["CSN", "Time"])
    else:
        dx = pd.merge(df1, df2, left_on=["UMid", "Time"], right_on=["UMid", "Time"])

    print("Agreement:", (dx.Location == dx.Room1).mean())

    if xn == "patient":
        df1["ClarityStart"] = pd.to_datetime(df1.ClarityStart)
        df1["ClarityEnd"] = pd.to_datetime(df1.ClarityEnd)
        print((df1.Time >= df1.ClarityStart).mean())
        print((df1.Time <= df1.ClarityEnd).mean())
