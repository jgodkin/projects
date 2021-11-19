'''James Godkin'''
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
import folium
from folium.plugins import MarkerCluster
from shapely.geometry import Point, LineString, Polygon
import json
import geopandas as gpd
import geoplot
import geoplot.crs as gcrs
import bokeh
from bokeh.plotting import figure, show, output_file

# Part 1
'''Assignment 5, Part1:Using the file capitals_lat_lon.csv, form a DataFrame,and write out an HTML file that when
    rendered in a browser displays a marker for each capital city'''


def part1():
    """This functions makes a map with all the capitals"""

    # url = 'https://raw.githubusercontent.com/SamanAl/Winter-Quarter-DU/master/Sample%20Data/capitals_lat_lon_csv.txt?token=ASPCIBFAGYBETQBLWVKKYI3AI274S'
    lat_lon_df = pd.read_csv('capitals_lat_lon_csv.txt', delimiter='\t')

    lat_lon_list = lat_lon_df[['Latitude', 'Longitude']].values.tolist()
    capital_list = lat_lon_df['Capital'].values.tolist()
    m = folium.Map(location=[14.59951, 120.98422], tiles='Cartodb Positron', zoom_start=4)

    marker_cluster = MarkerCluster(
        name='Capitals',
        overlay=True,
        control=False,
        icon_create_function=None
    )

    for k in range(len(lat_lon_list)):
        location = lat_lon_list[k]
        marker = folium.Marker(location=location, popup=capital_list[k])
        marker_cluster.add_child(marker)

    marker_cluster.add_to(m)
    folium.LayerControl().add_to(m)
    m.save(outfile="Godkin_James_Assignment5_Part1.html")


# Part 2
'''Assignment 5, Part 2:Estimate the latitude and longitude for three points of a triangle that would just cover
    Africa or come close. Use folium to demonstrate where the points are on the map so that a person could see at
    a glance that they do nearly cover Africa. Use Shapely to define a polygon from those three points.Compute the
    area and perimeter of the triangle assuming flat earth and allowing each degree of latitude and each degree
    of longitude to be considered one unit of length.'''


def part2():
    """This functions displays a map of Africa with a triangle that nearly covers it and displays the area and
        length in the console."""
    tri = [[35.7595, -5.8340], [-34.1747, 22.0834], [11.2755, 49.1879]]
    m = folium.Map(location=[4.3947, 18.5582], tiles='Cartodb Positron', zoom_start=3)
    for c in tri:
        m.add_child(folium.Marker(location=c, icon=folium.Icon(color='blue', icon="cloud")))
    q = LineString([(-5.8340, 35.7595), (22.0834, -34.1747), (49.1879, 11.2755)])
    r = Polygon(q)
    folium.GeoJson(data=r).add_to(m)
    print('Assignment5_Part2 Area = ', r.area, 'Length = ', r.length)
    m.save(outfile="Godkin_James_Assignment5_Part2.html")


# Part 3
'''Assignment 5, Part 3:Estimate the boundaries for Kansas (use just four lat-lon points) and Nebraska 
    (use just six points).Construct a GeoJSONfile from that.Then write a Python program to read that file, 
    form a dictionary, and plot the result.'''


def part3():
    """The function creates a json file and then plots that file to make a really odd looking Kansas and Nebraska"""
    geojson = {
    "type": "FeatureCollection","features": [
    { "type": "Feature", "id": "Kansas", "properties": { "rank": 1 },
    "geometry": { "type": "Polygon", "coordinates": [[[-102.05,40.005],[-102.05,37], [-94.63,37], [-95.439,40.005]]] }},
    { "type": "Feature", "id": "Nebraska ", "properties": { "rank": 2 },
    "geometry": { "type": "Polygon", "coordinates": [[[-102.05,40.005],[-95.439,40.005], [-96.5,42.5], [-104.12,42.97],
                                                      [-104.12,41], [-102.05,41]]] }} ]
    }
    with open('Godkin_James_Assignment5_Part3.json', 'w') as fp:
        json.dump(geojson, fp)

    df = gpd.read_file('Godkin_James_Assignment5_Part3.json')
    df.plot(figsize=(15, 9), alpha=0.9, edgecolor='k')
    plt.tight_layout()
    plt.savefig('Godkin_James_Assignment5_Part3.png')
    plt.show()


# Part 4
'''Assignment 5, Part 4:Generate at random the “happiness index” for each state in the United States.Provide a 
    choropleth of the United States with that data represented thereon.'''


def part4():
    """This function creates a happiness plot"""
    np.random.seed(52637)
    happyness = pd.Series(np.random.rand(51) * 100)
    happyness = pd.DataFrame(happyness, columns=['happyness'])
    happyness.reset_index(inplace=True)
    happyness = happyness.rename(columns={'index': 'id'})
    # state_url = (
    #    'https://raw.githubusercontent.com/SamanAl/Winter-Quarter-DU/master/Sample%20Data/states_ids.csv?token=ASPCIBFQVVK7RSEGZE2BX5DAJOSC2')
    state_id = pd.read_csv('states_ids.csv', delimiter=',', names=['id', 'state_id'])
    happyness = happyness.merge(state_id, left_on='id', right_on='id')

    url = ("https://raw.githubusercontent.com/python-visualization/folium/master/examples/data")
    state_geo = f"{url}/us-states.json"
    m = folium.Map(location=[40, -95], zoom_start=3)
    folium.Choropleth(
        geo_data=state_geo,
        name="choropleth",
        data=happyness,
        columns=['state_id', 'happyness'],
        key_on="feature.id",
        fill_color="RdPu",
        fill_opacity=0.7,
        line_opacity=.1,
        legend_name="Happiness Rate",
    ).add_to(m)

    folium.LayerControl().add_to(m)
    m.save("Godkin_James_Assignment5_Part4.html")

# Part 5
'''Assignment 5, Part 5: Generate 500 points from the random exponential distribution. Use Bokeh to plot a 
    histogram of that distribution such that there are at least 15 bins with counts greater than zero'''


def part5():
    """This function creates a histogram in Bokeh"""
    output_file('Godkin_James_Assignment5_Part5.html')
    np.random.seed(52637)
    x = 10 * np.random.randn(501)
    xx, edges = np.histogram(x, bins=15, range=[0, x.max()])
    df = pd.DataFrame({'xx': xx, 'left': edges[:-1], 'right': edges[1:]})
    p = figure(plot_width=1300, plot_height=600)
    p.quad(bottom=0, top=df['xx'], left=df['left'], right=df['right'], fill_color='lightgreen', line_color='black')
    show(p)


def main():
    part1()
    part2()
    part3()
    part4()
    part5()


if __name__ == '__main__':
    main()
