import matplotlib.pyplot as plt
import pandas as pd

from common import set_font_size, styles, put_lines_on_legend

scenarios = ["static", "slide", "fadein", "fadeout", "split", "merge"]

def get_change(row, scenario):
    if scenario == "merge":
        if row["delta-time"] == 29:
            return "OMRk++"
        elif row["delta-time"] == 41:
            return "DSC+"
        elif row["delta-time"] == 50:
            return "FEAC-S"
        elif row["delta-time"] == 40:
            return "DSC"
        return ""
    elif scenario == "split":
        if row["delta-time"] == 45:
            return "OMRk++"
        elif row["delta-time"] == 50:
            return "DSC"
        elif row["delta-time"] == 31:
            return "DSC+"
        return ""
    elif scenario == "fadein":
        if row["delta-time"] == 32:
            return "OMRk++"
        elif row["delta-time"] == 13:
            return "DSC+"
        elif row["delta-time"] == 21:
            return "DSC"
        return ""
    elif scenario == "fadeout":
        if row["delta-time"] == 17:
            return "OMRk++"
        elif row["delta-time"] == 19:
            return "DSC_DSC+_FEAC-S"
        return ""
    return ""

set_font_size(28)
fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(18, 10/2*3))

dfs = {}
for s, scenario in enumerate(scenarios):
    df = pd.read_csv(f"stream-profiling/{scenario}-common1-fixed0.9/seeds_SIM/frequency_statistics.csv", sep=',', quotechar='"', decimal='.')

    cluster_types = df["cluster-type"].unique()
    if "OMRkpp(SSE,1.0)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({"OMRkpp(SSE,1.0)": "OMRk++"})
    elif "OMRkpp(noname,SSE,1.0)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({"OMRkpp(noname,SSE,1.0)": "OMRk++"})
    df["cluster-type"] = df["cluster-type"].replace({
        "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,false,1.0,20)": "DSC",
        "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,true,1.0,20)": "DSC+",
        "FEACS(10,10,0.5,None,Some(20))": "FEAC-S",
        "CSCS(15,100,None,SSE,STANDARD,Some(1.0),Some(20))": "CSCS1",
        "CSCS(15,300,None,SSE,STANDARD,Some(1.0),Some(20))": "CSCS3"})
    df = df[df["cluster-type"].isin(["OMRk++", "DSC", "DSC+", "FEAC-S", "CSCS1", "CSCS3"])]
    df['silhouette'] = df['silhouette'].apply(lambda x: x if pd.isna(x) or x >= 0 else 0)
    df['delta-time'] = (df['window_start'] - df.groupby('cluster-type')['window_start'].transform('min'))/10000
    df['time'] = df['totalTime']/1000

    df_res = df.groupby(["cluster-type"]).agg({"time": "mean", "k": "mean", "silhouette": "mean", "sse": "mean", "ARI": "mean"}).reset_index()
    scenario_latex = r"$D_{" + scenario + "}$"
    df_res["scenario"] = scenario_latex
    dfs[scenario] = df_res

    # Group by cluster-type and delta-time for the plot
    df = df.groupby(["cluster-type", "delta-time"]).agg({"silhouette": "mean"}).reset_index()

    t="delta-time"
    df['change'] = df.apply(lambda row: get_change(row, scenario), axis=1)
    i=0

    df['splitted_change'] = df['change'].astype(str).str.split('_')
    algorithms = df["cluster-type"].unique()
    # Use subplots
    row, col = divmod(s, 2)
    ax = axs[row, col]
    for a in algorithms:
        alg_df = df[df["cluster-type"] == a]
        ax.plot(alg_df[t], alg_df["silhouette"], label = a, color=styles[a]["color"], linestyle=styles[a]["linestyle"], linewidth=styles[a]["linewidth"])
        #change_df = alg_df[(alg_df.change == a)] if scenario != "fadeout" else alg_df[(alg_df.change.notna() & alg_df.change.str.contains(re.escape(a)))]
        change_df = alg_df[(alg_df.change.notna() & alg_df["splitted_change"].apply(lambda substrings: a in substrings))]

        if len(change_df) > 0:
            ax.scatter(change_df[t],change_df["silhouette"],marker=styles[a]["marker"], label=a, color=styles[a]["color"], linestyle=styles[a]["linestyle"],  linewidth=styles[a]["linewidth"])
    ax.set_xlabel("Pane index")
    ax.set_ylabel("$SS$")
    ax.set_xticks(range(0, 65, 5))
    ax.set_yticks([i/100 for i in range(0, 100, 25)])
    ax.set_ylim(ymin=0,ymax=0.87)
    ax.set_title(scenario_latex)

# Adjust layout and add a single legend
plt.tight_layout()
plt.subplots_adjust(hspace=0.5)
new_handles, new_labels = put_lines_on_legend(axs[0, 0])
fig.legend(new_handles, new_labels, loc="lower center", bbox_to_anchor=(0.5, -0.08), ncol=3, prop={'size': 28})

# Save and show the plot
plt.savefig("stream-profiling/graphs/fig_trend_synthetic_SS.pdf", bbox_inches='tight', pad_inches=0.1)
plt.close()

#iterate dfs dictionary to create a single dataframe and write it to a csv file
df = pd.concat(dfs.values())
# create a table where each line is a dataset and a different measure and each column is a different algorithm
df_pivot = df.pivot_table(index=["scenario"],
                               columns="cluster-type",
                               values=["time", "k", "silhouette", "sse", "ARI"],
                               aggfunc='mean')

df_pivot.columns = ['_'.join(col).strip() for col in df_pivot.columns.values]

df_final = pd.DataFrame()
measures_dict = {"time": "Time (s)", "k": "k", "silhouette": "SS", "sse": "SSE", "ARI": "ARI"}
for measure in measures_dict.keys():
    temp_df = df_pivot.loc[:, df_pivot.columns.str.startswith(measure)].copy()
    temp_df['Measure'] = measures_dict[measure]
    temp_df.columns = [col.replace(f'{measure}_', '') for col in temp_df.columns]
    df_final = pd.concat([df_final, temp_df.reset_index()])
df_final = df_final[['Measure'] + [col for col in df_final.columns if col != 'Measure']].reset_index(drop=True)
df_final = df_final[['scenario', 'Measure', 'OMRk++', 'DSC+', 'DSC', 'CSCS1', 'CSCS3', 'FEAC-S']]
#rename column scenario to Dataset
df_final.rename(columns={'scenario': 'Dataset'}, inplace=True)

#order the df by Dataset column considering first the static scenario
dataset_order = ["$D_{static}$", "$D_{fadein}$", "$D_{fadeout}$", "$D_{slide}$", "$D_{split}$", "$D_{merge}$"]
order_measures = measures_dict.values()

df_final['Measure_Order'] = df_final['Measure'].map({v: i for i, v in enumerate(order_measures)})
df_final['Dataset_Order'] = df_final['Dataset'].map({v: i for i, v in enumerate(dataset_order)})
df_final = df_final.sort_values(by=['Dataset_Order', 'Measure_Order'])
df_final = df_final.drop(columns=['Measure_Order', 'Dataset_Order'])
#truncate values to 2 decimal places
df_final = df_final.round(2)

df_final.to_csv("stream-profiling/tab_synthetic.csv", index = False)