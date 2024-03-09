#############################################################
## Weather Data Checking Class
##############################################################
# this script aims to change teh weather data so that it is in a long format instead of wide
# this should help later on for better data analysis

import pandas as pd
df_weather = pd.read_csv("data/historical_weather_data_annual.csv")
df_weather.info()

df_weather.columns = map(str.lower, df_weather.columns)

# getting to long format
# Melting the DataFrame to go from wide to long format
df_temp = df_weather.melt(id_vars=['province'], value_vars=[col for col in df_weather.columns if 'temp' in col], var_name='year', value_name='average_temp')
df_rain = df_weather.melt(id_vars=['province'], value_vars=[col for col in df_weather.columns if 'rain' in col], var_name='year', value_name='average_rain')

# Extract year from the 'year' column
df_temp['year'] = df_temp['year'].str.extract('(\d{4})').astype(int)
df_rain['year'] = df_rain['year'].str.extract('(\d{4})').astype(int)

# Drop the "_average_temp" and "_average_rain" from the 'year' columns to have only the year
df_temp['year'] = df_temp['year'].astype(str).str.replace('_average_temp', '')

df_rain['year'] = df_rain['year'].astype(str).str.replace('_average_rain', '')

# Merge the temperature and rainfall DataFrames on 'province' and 'year'
df_combined = pd.merge(df_temp, df_rain, on=['province', 'year'])

df_combined.head()

# Savve df combined
df_combined.to_csv("combined_weather_data.csv", index=False)


