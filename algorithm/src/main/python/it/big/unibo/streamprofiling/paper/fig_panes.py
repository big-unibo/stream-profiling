import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from common import set_font_size, styles, get_df_wp, sort_labels_and_handles

df = get_df_wp()
df = df[df["records"] == 10000]
scaling_factor=1
set_font_size(25*scaling_factor)
max_wp = df.records_elaborated.max()

df = df.groupby(["cluster-type", "records_elaborated"]).agg({"stream_rate": "mean", "simulation_time_sec": "mean"}).reset_index()

if 'OMRk++' in df["cluster-type"].unique():
    for alg in ['OMRk++']:
        alg_row = df[df["cluster-type"] == alg].iloc[0]
        for wp in df.records_elaborated.unique():
            if wp < max_wp:
                df = pd.concat([pd.DataFrame([[alg, wp, alg_row['simulation_time_sec'], alg_row['stream_rate']]], columns=df.columns), df], axis=0, ignore_index=True)

plt.clf()
fig = plt.figure(figsize = (7*scaling_factor,5*scaling_factor))
for a in df["cluster-type"].unique():
    alg_df = df[df["cluster-type"] == a]
    plt.plot(alg_df["records_elaborated"], alg_df["stream_rate"], label = a, color=styles[a]["color"], linestyle=styles[a]["linestyle"], linewidth=styles[a]["linewidth"]*scaling_factor)
texts=[]

# Add labels for each point
for i, row in df.iterrows():
    flag_offset_y=300
    flag_offset_x = 0
    if row["cluster-type"] == "DSC+":
        if row["records_elaborated"] == 500:
            flag_offset_y=2000
        elif row["records_elaborated"] < 10000:
            flag_offset_y=1000
            if row["records_elaborated"] == 2000:
                flag_offset_x = -50
                flag_offset_y=800
        else:
            flag_offset_y=-1000
    if row["cluster-type"] == "FEAC-S":
        if row["records_elaborated"] == 500:
            flag_offset_y=1500
        elif row["records_elaborated"] == 1000:
            flag_offset_y=1700
        elif row["records_elaborated"] < 5000:
            flag_offset_y=1900
        elif row["records_elaborated"] == 5000:
            flag_offset_y=800
    if row["cluster-type"] == "CSCS":
        if row["records_elaborated"] == 500:
            flag_offset_y=1100
        elif row["records_elaborated"] < 5000:
            flag_offset_y=700
        elif row["records_elaborated"] == 5000:
            flag_offset_y=1300
    if row["cluster-type"] == "DSC":
        if row["records_elaborated"] == 500:
            flag_offset_y=900
        elif row["records_elaborated"] <= 5000:
            flag_offset_y=700

    texts.append(plt.text(row["records_elaborated"] + flag_offset_x, row["stream_rate"] + flag_offset_y, f'{row["simulation_time_sec"]:.0f}' if row["cluster-type"] == "OMRk++" else f'{row["simulation_time_sec"]:.1f}', \
                          ha='center', fontsize=12*scaling_factor, bbox=dict(facecolor='none', edgecolor=styles[row["cluster-type"]]["color"], pad=1.0)))
    plt.scatter(row["records_elaborated"], row["stream_rate"], color=styles[row["cluster-type"]]["color"], s=10*scaling_factor)

plt.xlabel(r"Window period $w\_per$")
plt.ylabel(r"Max. rate (schemas/s)")
plt.xticks(np.sort(df.records_elaborated.unique()), [x if x not in [1000, 2000] else "" for x in np.sort(df.records_elaborated.unique())])
plt.yticks(range(0, 22000, 2000))
# Set the minimum value of the y-axis to 0
plt.ylim(ymin=0)
plt.legend()#loc="lower right",
handles, labels = plt.gca().get_legend_handles_labels()
labels, handles = sort_labels_and_handles(labels, handles)
plt.legend(handles, labels, prop={'size': 12*scaling_factor}, ncol=1)
fig.tight_layout()
plt.savefig("stream-profiling/graphs/fig_panes.pdf") # Save to pdf
plt.close()