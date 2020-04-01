import geopandas as gpd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('files', nargs='*')
args = parser.parse_args()

files = args.files
df = gpd.read_file(os.path.join('data', files[0], files[0] + '.gpkg'))
df['subject'] = files[0]
for file in files[1:]:
    topic_df = gpd.read_file(os.path.join('data', file, file + '.gpkg'))
    topic_df['subject'] = file
    df = df.append(topic_df, ignore_index=True)

filename = '-'.join([s[:2] for s in files])
df.to_file(os.path.join('data', filename + '.gpkg'), driver='GPKG')