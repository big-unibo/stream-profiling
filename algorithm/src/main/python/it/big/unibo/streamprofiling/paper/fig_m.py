import matplotlib.pyplot as plt
import pandas as pd

from common import set_font_size, styles, get_algorithm, get_m, sort_labels_and_handles

set_font_size(25)
df = pd.read_csv("stream-profiling/algoParameters/seeds_SIM/frequency_statistics.csv", sep=',', quotechar='"', decimal='.')
df["algorithm"] = df["cluster-type"].apply(lambda a: get_algorithm(a))
df["m"] = df["cluster-type"].apply(lambda a: get_m(a)).astype(int)
# filter out the records with the column algorithm with the string SS
df = df[~df["algorithm"].str.contains("SS")]
# filter out when m=3
df = df[~(df["m"] == 3)]
df = df.groupby(["algorithm", "m"]).agg({"silhouette": "mean"}).reset_index()
for alg in ['OMRk++', 'FEAC-S']:
    alg_row = df[df["algorithm"] == alg].iloc[0]
    for m in sorted(df.m.unique()):
        if m < 400:
            df = pd.concat([pd.DataFrame([[alg, m, alg_row["silhouette"]]], columns=df.columns), df], axis=0, ignore_index=True)
plt.clf()

fig = plt.figure(figsize = (7,5))
for alg in df.algorithm.unique():
    alg_df = df[df.algorithm == alg]
    plt.plot(alg_df["m"], alg_df["silhouette"], label = alg, color=styles[alg]["color"], linestyle=styles[alg]["linestyle"], linewidth=styles[alg]["linewidth"])
plt.xlabel("Coreset size $m$")
# Set the minimum value of the y-axis to 0
plt.ylim(ymin=0)
plt.xticks([0] + [m for m in df.m.unique() if m > 25])
#plt.yticks([i/10 for i in range(-1, 11, 1)])
plt.ylabel("Average $SS$")
plt.legend(loc="lower right", prop={'size': 15})#borderaxespad=0., bbox_to_anchor=(1.1, 0.5, 1, .5), title = "Algorithms", loc=3, ncol=1)
handles, labels = plt.gca().get_legend_handles_labels()
labels, handles = sort_labels_and_handles(labels, handles)
plt.legend(handles, labels, prop={'size': 12})
fig.tight_layout()
plt.savefig("stream-profiling/graphs/fig_m.pdf") # Save to pdf
plt.close()