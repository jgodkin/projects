'''James Godkin'''
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import geoplot
import json


def grey(df_states):
    df_states.plot(column='debtfree',
                   figsize=(15, 8),
                   legend=True,
                   edgecolor="black",
                   cmap='binary',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    plt.title("Year of state law passage protecting married women’s separate property from her husband’s debts", fontsize=20)
    plt.xticks([])
    plt.yticks([])
    plt.tight_layout()
    plt.show()

    df_states.plot(column='effectivemwpa',
                   figsize=(15, 8),
                   legend=True,
                   edgecolor="black",
                   cmap='binary',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    plt.title("Year of state law passage granting married women control and management rights over their separate property", fontsize=18)
    plt.xticks([])
    plt.yticks([])
    plt.tight_layout()
    plt.show()

    df_states.plot(column='earnings',
                   figsize=(15, 8),
                   legend=True,
                   edgecolor="black",
                   cmap='binary',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    plt.title("Year of state law passage granting married women ownership of their wages or earnings on par with other separate property", fontsize=16)
    plt.xticks([])
    plt.yticks([])
    plt.tight_layout()
    plt.show()

    df_states.plot(column='wills',
                   figsize=(15, 8),
                   legend=True,
                   edgecolor="black",
                   cmap='binary',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    plt.title("Year of state law passage granting married women the ability to write wills without their husband's consent or other restrictions", fontsize=16)
    plt.xticks([])
    plt.yticks([])
    plt.tight_layout()
    plt.show()

    df_states.plot(column='soletrader',
                   figsize=(15, 8),
                   legend=True,
                   edgecolor="black",
                   cmap='binary',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    plt.title("Year of state law passage granting married women as a class the right to sign contracts and engage in business without consent of husband", fontsize=14)
    plt.xticks([])
    plt.yticks([])
    plt.tight_layout()
    plt.show()


def color(df_states):
    fig, axs = plt.subplots(5, 1, figsize=(15, 50))
    df_states.plot(
        column='debtfree',
        figsize=(15, 8),
        ax=axs[0],
        legend=True,
        edgecolor="black",
        cmap='viridis',
        missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
        legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"}
    )
    axs[0].axis('off')
    axs[0].set_title("Year of state law passage protecting married women’s separate property from her husband’s debts")

    df_states.plot(column='effectivemwpa',
                   figsize=(15, 8),
                   ax=axs[1],
                   legend=True,
                   edgecolor="black",
                   cmap='viridis',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    axs[1].axis('off')
    axs[1].set_title(
        "Year of state law passage granting married women control and management rights over their separate property")

    df_states.plot(column='earnings',
                   figsize=(15, 8),
                   ax=axs[2],
                   legend=True,
                   edgecolor="black",
                   cmap='viridis',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    axs[2].axis('off')
    axs[2].set_title(
        "Year of state law passage granting married women ownership of their wages or earnings on par with other separate property")

    df_states.plot(column='wills',
                   figsize=(15, 8),
                   ax=axs[3],
                   legend=True,
                   edgecolor="black",
                   cmap='viridis',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    axs[3].axis('off')
    axs[3].set_title(
        "Year of state law passage granting married women the ability to write wills without their husband's consent or other restrictions")

    df_states.plot(column='soletrader',
                   figsize=(15, 8),
                   ax=axs[4],
                   legend=True,
                   edgecolor="black",
                   cmap='viridis',
                   missing_kwds={'color': 'white', "hatch": "///", "label": "Missing values", },
                   legend_kwds={'label': "Hatched Means Law Was Never Passed", 'orientation': "horizontal"})
    axs[4].axis('off')
    axs[4].set_title(
        "Year of state law passage granting married women as a class the right to sign contracts and engage in business without consent of husband")

    plt.xticks([])
    plt.yticks([])
    plt.suptitle('Married Women’s Economic Rights Reform, 1835-1920', fontsize=30, y=1)
    plt.tight_layout()
    plt.show()


def main():
    url = 'https://raw.githubusercontent.com/SamanAl/Winter-Quarter-DU/master/Sample%20Data/SturmData_csv.txt?token=ASPCIBCNYGG6CJR72CTF44LAKY6CK'
    df = pd.read_csv(url)
    # df = pd.read_csv('SturmData_csv.txt', delimiter=',')
    url = 'https://raw.githubusercontent.com/SamanAl/Winter-Quarter-DU/master/Sample%20Data/states_ids.csv?token=ASPCIBGCHGZFTZVLGAS4PPDAKY6FY'
    state_code = pd.read_csv(url, header=None)
    #state_code = pd.read_csv('states_ids.csv', header=None)
    state_code = state_code.drop([3, 43, 50])
    df = df.merge(state_code, left_on='state', right_on=1)
    df = df.drop(columns=[1])
    df = df.rename(columns={0: 'map_id'})
    df = df.sort_values(['map_id'])
    df = df.reset_index(drop=True)
    df_states = gpd.read_file(geoplot.datasets.get_path('contiguous_usa'))
    df_states['debtfree'] = df['debtfree']
    df_states['effectivemwpa'] = df['effectivemwpa']
    df_states['earnings'] = df['earnings']
    df_states['wills'] = df['wills']
    df_states['soletrader'] = df['soletrader']

    grey(df_states)  # grey scale for printed or colorless publication
    #color(df_states)  # color version of plots with all maps on one plot however when ran from the cmd the plots,
                       # overlap however ran any where else plots look great


if __name__ == '__main__':
    main()
