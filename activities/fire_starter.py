import pandas as pd
from fire import Fire

df = pd.DataFrame({
    "player": ["ovi", "mcjesus", "tavares", "muzzin"],
    "goals": [51, 41, 47, 9]
    })

player_nickname = "ovi"

def goals(player_nickname):
    return df[df.player == player_nickname]["goals"].values[0]

goals("ovi")

# <your code here>
# take the goals function and turn it into a cli with fire
# try to access "mcjesus" aka (Connor McDavid)
