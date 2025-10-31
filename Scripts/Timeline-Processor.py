import os
import json
import pandas as pd
from IPython.display import display
from pandas.core.interchange.dataframe_protocol import DataFrame

rootdir = "C:/Api_Data/timeline_data"

csvfilepath = "../TestCSV/"

def dump(inp):
    print(json.dumps(inp,indent=2))

def load_timeline():
    matches = 0
    for subdir, dirs, files in os.walk(rootdir):
        df_dict = {"puuid": [], "matchId": [], "timestamp": [], "currentGold": [], "goldPerSecond": [],
                   "totalGold": [], "championDamage": [], "minionsKilled": [], "participantId": [], "position": [],
                   "xp": [], "level": []}
        for file in files:
            parts = file.rsplit('_', 1)
            matchId = parts[0]
            filepath = subdir+"/"+file

            with open(filepath, "r") as f:
                data = json.load(f)
                participants_dict = data["info"]["participants"]
                for frame in data["info"]["frames"]:
                    for k,v in frame["participantFrames"].items():
                        df_dict["puuid"].append(participants_dict[int(v["participantId"]) - 1]["puuid"])
                        df_dict["matchId"].append(matchId)
                        df_dict["timestamp"].append(frame["timestamp"])
                        df_dict["currentGold"].append(v["currentGold"])
                        df_dict["goldPerSecond"].append(v["goldPerSecond"])
                        df_dict["totalGold"].append(v["totalGold"])
                        df_dict["championDamage"].append(v["damageStats"]["totalDamageDoneToChampions"])
                        df_dict["minionsKilled"].append(v["minionsKilled"])
                        df_dict["participantId"].append(v["participantId"])
                        df_dict["position"].append(v["position"])
                        df_dict["xp"].append(v["xp"])
                        df_dict["level"].append(v["level"])
                matches += 1
                print("written " + str(matches) + " matches")
                if matches % 100 == 0:
                    df = pd.DataFrame(df_dict)
                    df.to_csv('../TestCSV/participant_frames.csv', mode='a',header=not os.path.exists('../TestCSV/participant_frames.csv'), index=False)

load_timeline()
