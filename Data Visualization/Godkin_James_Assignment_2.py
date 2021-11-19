'''James Godkin'''
import csv
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import yfinance as yf

# Part 1
'''Assignment 2, Part 1: Create a list of the atomic weights of the first six elements of the periodic table,
    each rounded to the nearest integer.Provide two pie charts as follows: (1) each slice annotated with a 
    percentage of the whole and (2) each slice annotated with its atomic weight.Explode a different element with
    each chart'''
def part1():
    """The function creates 2 plots of the first 6 atomic weights. The first one shows the percentage. The second
    is the weight"""
    # Data to plot
    atomic_name = ["Hydrogen", "Helium", "Lithium", "Beryllium", "Boron", "Carbon"]
    atomic_weight = [1, 4, 7, 9, 11, 12]
    colors = ['#e6ab83', '#c95001', '#75396c', '#226b7c', '#d1e4de', '#5c7772']
    explode = (0, 0, 0, 0, 0.07, 0)  # explode 5th slice
    total = sum(atomic_weight)

    # Plot 1
    plt.pie(atomic_weight, explode=explode, labels=atomic_name, colors=colors,
            autopct='%1.1f%%', startangle=140)

    plt.axis('equal')
    plt.show()

    # Plot 2
    explode = (0, 0, 0.07, 0, 0, 0)  # explode 3rd slice
    plt.pie(atomic_weight, explode=explode, labels=atomic_name, colors=colors,
            autopct=lambda pct: int(pct*total/100), startangle=140)

    plt.axis('equal')
    plt.show()

# Part 2
'''Assignment 2, Part 2: Read into a DataFrame the file py_ide2.csv,and provide both a horizontal bar chart and
    a vertical bar chart, complete with all labels.Be sure to rotate the IDE names so that they are readable.'''
def part2():
    """This functions reads in a CSV file and creates two bar graphs"""
    url = 'https://raw.githubusercontent.com/SamanAl/Winter-Quarter-DU/master/Sample%20Data/py_ide2.csv?token=ASPCIBGA2D7PNHMKKWN5ZLLAC4JFE'
    df = pd.read_csv(url)

    df.plot.bar(x='IDE', y='Adoption', rot=50, title='Adoption Rate of IDE')
    plt.ylabel='Percentage'
    plt.show()

    df.plot.barh(x='IDE', y='Adoption', title='Adoption Rate of IDE')
    plt.show()

# Part 3
'''Construct a list of eight strings that represent days evenly spread out. Drawing from the random uniform
    distribution, make an array of eight floats ranging from 100 to 200 in value. Establish a DataFrame from that
    list and that array, convert the dates to pandas datetime objects,and set them to the index.Make two charts
    in the same window or canvas as follows: (1) a line plot of the values vs. dates and (2) a bar chart of the
    same.'''
def part3():
    """The function creates a list of days then a list of floats converts them into time then graphs it over a bar and
    line plot"""
    #Creating the data
    days = ['2021-01-07', '2021-01-14', '2021-01-07', '2021-01-21', '2021-01-28', '2021-02-04', '2021-02-11', '2021-02-18']
    random_num = np.random.uniform(low=100, high=200, size=8)
    days_units_df = pd.DataFrame(data={'days': days, 'units': random_num})
    days_units_df['days'] = pd.to_datetime(days_units_df['days'])
    days_units_df = days_units_df.set_index('days')
    #Plot
    fig, axes = plt.subplots(nrows=2, ncols=1)
    days_units_df.plot.bar(y='units', ax=axes[0], rot=1)
    days_units_df.plot(y='units', ax=axes[1])
    plt.show()


# Part 4
'''Assignment 2, Part 4: Pull from Yahoo!Finance the closing prices and volumes of the stock of your choice 
    over the trading days of one month,and plot the prices and volumes on a canvas in two separate panels, one above
    the other, with the dates aligned.'''
def part4():
    """This function gets the data of a stock from Yahoo and plots them"""
    stock_df=yf.download('GME','2020-12-26', '2021-01-26')
    #Plot
    fig, axes = plt.subplots(2,1,figsize=(20,20))
    stock_df.plot(y='Close', color='r', ax=axes[0], title='Gamestop Closing Stock Price Dec - Jan', legend=False)
    axes[0].grid(b=True, which='minor', color='#999999', linestyle='-')
    axes[0].set_ylabel('Closing Price')
    stock_df.plot(y='Volume', color='r', ax=axes[1], title='Gamestop Stock Volume Dec - Jan', legend=False)
    axes[1].grid(b=True, which='minor', color='#999999', linestyle='-')
    axes[1].set_ylabel('Volume')
    plt.show()

def main():
    part1()
    part2()
    part3()
    part4()

if __name__ == '__main__':
    main()
