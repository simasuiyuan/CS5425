#%%
import plotly.express as px
import pandas as pd
import numpy as np
# %%
d = {'cluster': [i for i in range(25)], 'runtime': [np.nan,np.nan,27,13,56,35,28,41,30,10,np.nan, np.nan,np.nan,24, 
np.nan,30, np.nan,28,29,23,np.nan,17,np.nan,42,27]}
# 0:
# 1:
# 2: 27, 46
# 3: 13, 16
# 4: 56
# 5: 35
# 6: 28
# 7: 41, 76
# 8: 30
# 9: 10
# 10:
# 11:
# 12:
# 13: 24, 34
# 14: 
# 15: 30, 49
# 16: 
# 17: 28
# 18: 29
# 19: 23
# 20: 
# 21: 17
# 22:
# 23: 42, 79, 77, 79
# 24: 27, 39

# 2   images_3490fd706da6b61e    24    a couple of young people with giraffes in a field. 

# 7   images_393cb7feb10902b5    7     a view of a red truck sitting in a driver's seat [UNK] for the man cross in the back.

# 8   images_28dd9bb73072d6c6    19    a dog stands near a silver handle

# 1   images_3567f456130c4ec2    23    a person taking a face looking at a cell phone that is shown.

# 10  images_1c27e643d5bd43bb    23    there are perched on top of each together.

# 6   images_141a942ad44d3080    4     a sign on a table tasting as the side of a restaurant.

# 14  images_27f0769af257cfd3    3    a man is snow board [UNK] in between two people with decorated with red

# 3   images_0c19ba009c91e5bb    7    a street with cars that has a full of trucks.

# 9   images_2f3e2cb599c43075    9    a long train crossing a bridge above the water.

# 5   images_1f990b331e554127    5    a stove with [UNK] carved [UNK] eyes in a kitchen, two adorned with a stove a smoke [UNK]

# 0   images_3313635dda11cb72    17    a man standing on a black suits stuck into a hospital [UNK] from a business

# 4   images_1166b9acbbb80436    21    a [UNK] by a loaf has a [UNK] and tomatoes, carrots, banana, cheese, and green apple

# 15  images_000000250055        15    a woman sitting on a bench.

# 16  images_000000247396        3    two men on a snowboard while taking pics

# 13  images_000000165054        13    a [UNK] picture of the skateboard skate park.

# 17  images_000000031311        23    a person sitting on a person standing next to two people have their phones

# 18  images_000000038885        18   four engine airliner is parked as it its loading up off of it.
 
# 11  images_000000113754         2  an outside vase sitting in a field and flowers.

# 12  images_000000278444         13  a man and a woman playing a game with wii remotes.

# 19  images_000000275507         8  a young woman standing [UNK] to sitting in the surf board in a boat

# 20  images_000000186505         6  a man leaning on a couch looking around the end of a wooden door.

# 21   images_16b5911ec2643687    2  a tree branch on the ground in the dirt.

# 22   images_000000010194       23  uniform that has hit the ball.

# 23   images_000000269509       15  a police officer sitting on a motorcycle outside

# 24   images_000000189358       24  the zebras work with a rose in the background.
# %%
import plotly.express as px

fig = px.line(d, x="cluster", y="runtime")
fig.show()
# %%
