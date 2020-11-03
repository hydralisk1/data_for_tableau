from glob import glob
import pandas as pd
from multiprocessing import Pool
import csv

def cleaning_data(filename):
    df = pd.read_csv(filename)
    df.columns = ["tripduration", "starttime", "stoptime", "start station id", "start station name", "start station latitude",
                  "start station longitude", "end station id", "end station name", "end station latitude", "end station longitude",
                  "bikeid", "usertype", "birth year", "gender"]

    df = df.drop(columns=["stoptime", "start station id", "start station name", "start station latitude", "start station longitude",
                          "end station id", "end station name", "end station latitude", "end station longitude", "bikeid", "birth year"])

    df["starttime"] = df.starttime.str.split(" ").str[0]+" "+df.starttime.str.split(" ").str[1].str.split(":").str[0]+":00:00"
    date_time = df["starttime"].unique()
    total_count = []
    male_count = []
    female_count = []
    total_duration = []
    male_duration = []
    female_duration = []
    subscriber_count = []
    subscriber_duration = []
    n_subscriber_count = []
    n_subscriber_duration = []
    
    for d in date_time:
        groupby_g = df.loc[df["starttime"] == d].groupby("gender")
        groupby_u = df.loc[df["starttime"] == d].groupby("usertype")
        total_count.append(df.loc[df["starttime"] == d].tripduration.count())
        total_duration.append(df.loc[df["starttime"] == d].tripduration.sum())
        
        try:
            subscriber_count.append(groupby_u.count().loc["Subscriber", "tripduration"])
            subscriber_duration.append(groupby_u.sum().loc["Subscriber", "tripduration"])
        except KeyError:
            subscriber_count.append(0)
            subscriber_duration.append(0)
        
        try:
            n_subscriber_count.append(groupby_u.count().loc["Customer", "tripduration"])
            n_subscriber_duration.append(groupby_u.sum().loc["Customer", "tripduration"])
        except KeyError:
            n_subscriber_count.append(0)
            n_subscriber_duration.append(0)
        
        try:
            male_count.append(groupby_g.count().loc[1, "tripduration"])
            male_duration.append(groupby_g.sum().loc[1, "tripduration"])
        except KeyError:
            male_count.append(0)
            male_duration.append(0)
            
        try:
            female_duration.append(groupby_g.sum().loc[2, "tripduration"])
            female_count.append(groupby_g.count().loc[2, "tripduration"])
        except KeyError:
            female_duration.append(0)
            female_count.append(0)
        
    temp_df = pd.DataFrame({
        "date_time": date_time,
        "count": total_count,
        "duration": total_duration,
        "male_count": male_count,
        "female_count": female_count,
        "male_duration": male_duration,
        "female_duration": female_duration,
        "subscriber_count": subscriber_count,
        "non-subscriber_count": n_subscriber_count,
        "subscriber_duration": subscriber_duration,
        "non-subscriber_duration": n_subscriber_duration
    })
    print(f"{filename} completed")

    temp_df.to_csv('cleaned_csv/data.csv', mode="a", header=False, index=False)

def main():
    csv_files = glob("csv/*.csv")

    header = ["date_time", "count", "duration", "male_count", "female_count",
              "male_duration", "female_duration", "subscriber_count",
              "non-subscriber_count", "subscriber_duration", "non-subscriber_duration"]

    with open('cleaned_csv/data.csv', 'w', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(header)

    with Pool(processes=4) as pool:
        pool.map(cleaning_data, csv_files)
            

if __name__ == '__main__':
    main()