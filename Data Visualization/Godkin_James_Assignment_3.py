'''James Godkin'''
import csv
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
print(sns.__version__)

# Part 1
'''Assignment 3, Part 1:Using the built-in Seaborn dataset mpg, provide a heatmap of the correlation of all 
    the numeric columns and provide a pair plot of the same.'''
def part1():
    '''This functions makes a heat map and pairplot of mpg dataset'''
    mpg_df = sns.load_dataset('mpg')
    mpg_df_corr_matrix = mpg_df.corr()
    plt.figure(figsize=(10, 8))
    ax = plt.axes()
    sns.heatmap(mpg_df_corr_matrix, annot=True, cmap='coolwarm', ax=ax)
    ax.set_title('Heatmap of MPG')
    plt.show()
    g = sns.pairplot(mpg_df)
    g.fig.suptitle('Pair Plot of MPG', y=1.02)
    plt.show()

# Part 2
'''Assignment 3, Part 2:Using the built-in Seaborn dataset diamonds, establish a FacetGrid basedon ‘cut’ and
    ‘color’. Eliminate colors ‘D’ and ‘E’ as well as the cut ‘Fair’.Within that grid, plot the scatterplot
    for‘price’ vs. ‘carat’.'''
def part2():
    """This functions makes a facetgrid scatterplot of diamonds data set"""
    diamonds_df = sns.load_dataset('diamonds')
    diamonds_df.drop(diamonds_df[diamonds_df['color'] == 'D'].index, inplace=True)
    diamonds_df.drop(diamonds_df[diamonds_df['color'] == 'E'].index, inplace=True)
    diamonds_df.drop(diamonds_df[diamonds_df['cut'] == 'Fair'].index, inplace=True)
    diamonds_plot = sns.FacetGrid(diamonds_df, col='cut',  row='color')
    diamonds_plot.map(sns.scatterplot, 'price', 'carat')
    diamonds_plot.set_axis_labels('Price', 'Carat')
    diamonds_plot.set_titles(col_template='Color: {col_name}', row_template='Cut: {row_name}')
    plt.show()

# Part 3
'''Assignment 3, Part 3:Using the built-in Seaborn dataset car_crashes, prepare plots with a scattergram with
    the linear model for both the total vs. speeding and the total vs. alcohol.'''
def part3():
    """The function creates a scattergram of the car_crashes data"""
    car_crashes_df = sns.load_dataset('car_crashes')
    fig, axes = plt.subplots(nrows=2, ncols=1)
    # regplot was chosen over lmplot to plot both graphs in the same subplot
    sns.regplot(x='total', y='speeding', ax=axes[0], data=car_crashes_df, color='g')
    sns.regplot(x='total', y='alcohol',ax=axes[1],  data=car_crashes_df)
    plt.show()


# Part 4
'''Assignment 4, Part 4:Using the built-in Seaborn dataset iris, provide a plot with four subplots where in
    the distribution of each of the numeric columns is presented as a set of boxplots, one for each ‘species’.'''
def part4():
    """This function creates boxplots of iris data"""
    iris_df = sns.load_dataset('iris')
    f, axes = plt.subplots(2, 2)
    sns.boxplot(x=iris_df['species'], y=iris_df['sepal_length'], ax=axes[0,0], data=iris_df)
    sns.boxplot(x=iris_df['species'], y=iris_df['sepal_width'], ax=axes[0,1], data=iris_df)
    sns.boxplot(x=iris_df['species'], y=iris_df['petal_length'], ax=axes[1,0], data=iris_df)
    sns.boxplot(x=iris_df['species'], y=iris_df['petal_width'], ax=axes[1,1], data=iris_df)
    plt.show()

def main():
    part1()
    part2()
    part3()
    part4()

if __name__ == '__main__':
    main()
