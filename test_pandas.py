import pandas as pd
import numpy as np

# we read in the data and send it to the sql database
df = pd.read_csv("california_housing_test.csv")

df.head()