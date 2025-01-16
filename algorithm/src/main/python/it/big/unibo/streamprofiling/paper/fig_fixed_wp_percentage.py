import matplotlib.pyplot as plt
import numpy as np

from common import set_font_size, styles, get_df_wp, sort_labels_and_handles

df = get_df_wp()
df = df[(df["records"] > 1000) & ((df["records"] == 10000) | (df["records"] == 100000) | (df["records"] == 1000000))]
df = df[((df["cluster-type"] == "OMRk++") & (df["records"] == 10000)) | (df["window_period_portion"] == 0.1)]
df = df.groupby(["cluster-type", "records"]).agg({"stream_rate": "mean", "simulation_time_sec": "mean"}).reset_index()

# Code for pyplot
scaling_factor=1
set_font_size(25*scaling_factor)
records_sorted = np.sort(df["records"].unique())
records_index_mapping = {record: i for i, record in enumerate(records_sorted)}
df["records_value"] = df["records"].apply(lambda x: records_index_mapping[x])
plt.clf()
fig = plt.figure(figsize = (7*scaling_factor,5*scaling_factor))
for a in df["cluster-type"].unique():
    alg_df = df[df["cluster-type"] == a]
    plt.plot(alg_df["records_value"], alg_df["stream_rate"], label = a, color=styles[a]["color"], linestyle=styles[a]["linestyle"], linewidth=styles[a]["linewidth"]*scaling_factor)
texts=[]
# Add labels for each point
for i, row in df.iterrows():
    flag_offset_y=200
    flag_offset_x = 0
    if row["cluster-type"] == "DSC+":
        if row["records"] > 1000:
            flag_offset_y = -500
        else:
            flag_offset_y = 1500
    if row["cluster-type"] == "FEAC-S":
        if row["records"] == 1000:
            flag_offset_y = 800
    if row["cluster-type"] == "CSCS":
        if row["records"] == 1000:
            flag_offset_y = 500
        elif row["records"] == 10000 or row["records"] == 1000000:
            flag_offset_y = -500
    if row["cluster-type"] == "DSC":
        if row["records"] == 10000:
            flag_offset_y = 300
        else:
            flag_offset_y = -400
    if row["cluster-type"] == "OMRk++" and row["records"] == 1000000:
        flag_offset_x = -0.05

    texts.append(plt.text(row["records_value"] + flag_offset_x, row["stream_rate"] + flag_offset_y, f'{row["simulation_time_sec"]:.0f}' if row["cluster-type"] == "OMRk++" else f'{row["simulation_time_sec"]:.1f}', \
                          ha='center', fontsize=12*scaling_factor, bbox=dict(facecolor='none', edgecolor=styles[row["cluster-type"]]["color"], pad=1.0)))
    plt.scatter(row["records_value"], row["stream_rate"], color=styles[row["cluster-type"]]["color"], s=10*scaling_factor)

plt.xlabel(r"Window length $w\_len$")
plt.ylabel(r"Max. rate (schemas/s)")
plt.xticks(list(records_index_mapping.values()), list(records_index_mapping.keys()))
plt.yticks([0, 2000, 4000, 6000, 8000])
# Set the minimum value of the y-axis to 0
plt.ylim(ymin=0)
plt.legend()#loc="lower right",
handles, labels = plt.gca().get_legend_handles_labels()
labels, handles = sort_labels_and_handles(labels, handles)
plt.legend(handles, labels, prop={'size': 12*scaling_factor}, ncol=1)
fig.tight_layout()
plt.savefig("stream-profiling/graphs/fig_fixed_wp_percentage_.pdf") # Save to pdf
plt.close()