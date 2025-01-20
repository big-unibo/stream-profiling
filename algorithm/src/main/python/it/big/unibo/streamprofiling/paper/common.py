import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import rc
from matplotlib.backends.backend_pdf import PdfPages

algorithms_order = ["OMRk++", "DSC+", "DSC", "CSCS", "CSCS1", "CSCS3", "FEAC-S"]

def savePdf(name, fig) :
  strFile = name + '.pdf'
  #if os.path.isfile(strFile):
  # os.remove(strFile)   # Opt.: os.system("rm "+strFile)
  pp = PdfPages(strFile)
  pp.savefig( fig, dpi=300, bbox_inches = "tight" )
  pp.close()
# Increasing the resolution of the plot
plt.figure(dpi=100)
rc('text', usetex=True)
rc('text.latex', preamble=r'\usepackage{amsmath} \usepackage{nicefrac}')
def set_font_size(size) :
  rc_fonts = {
    "text.usetex": True,
    "font.size": size,
    'mathtext.default': 'regular',
    'axes.titlesize': size,
    "axes.labelsize": size,
    "legend.fontsize": size,
    "xtick.labelsize": size,
    "ytick.labelsize": size,
    'figure.titlesize': size,
   # 'text.latex.preamble': "[r'\usepackage{amsmath,amssymb,bm,physics,lmodern}']",
    "font.family": "serif",
    "font.serif": "computer modern roman",
  }
  matplotlib.rcParams.update(rc_fonts)

def get_algorithm(alg):
    if "SSE" in alg and "CSCS" in alg:
        return "CSCS"
    if "CSCS" in alg:
        return "CSCS^{SS}"
    if "SSE" in alg and "DSC" in alg and "false" in alg:
        return "DSC"
    if "DSC" in alg and "false" in alg:
        return "DSC^{SS}"
    if "SSE" in alg and "DSC" in alg and "true" in alg:
        return "DSC+"
    if "DSC" in alg and "true" in alg:
        return "DSC+^{SS}"
    if "SSE" in alg and "OMRkpp" in alg:
        return "OMRk++"
    if "OMRkpp" in alg:
        return "OMRk++^{SS}"
    if "FEACS" in alg:
        return "FEAC-S"
    return alg

def get_m(alg):
    if "DSC" in alg or "CSCS" in alg:
        m = int(alg.strip().split("(")[1].split(",")[1])
        return m
    return 400

def get_df_wp():
    df = pd.read_csv("stream-profiling/windowTest/seeds_SIM/frequency_statistics.csv", sep=',', quotechar='"', decimal='.')
    # I consider the processed records in relation to the frequency
    df["records_elaborated"] = df["windowPeriod"] / df["data_frequency"] * df["seeds"]
    # convert column to integer
    df["records_elaborated"] = df["records_elaborated"].astype(int)
    df["simulation_time_sec"] = df["totalTime"] / 1000
    df["stream_rate"] = df["records_elaborated"] / df["simulation_time_sec"]
    df["window_period_portion"] = df["records_elaborated"]/df["records"]

    cluster_types = df["cluster-type"].unique()
    if "OMRkpp(SSE,1.0)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({"OMRkpp(SSE,1.0)": "OMRk++"})
    elif "OMRkpp(noname,SSE,1.0)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({"OMRkpp(noname,SSE,1.0)": "OMRk++"})
    if "DSC(15,100,SSE,CENTROID,-30,30,1.0,0.1,false,1.0,20)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({
            "DSC(15,100,SSE,CENTROID,-30,30,1.0,0.1,false,1.0,20)": "DSC"
        })
    elif "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,false,1.0,20)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({
            "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,false,1.0,20)": "DSC"
        })
    if "DSC(15,100,SSE,CENTROID,-30,30,1.0,0.1,true,1.0,20)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({
            "DSC(15,100,SSE,CENTROID,-30,30,1.0,0.1,true,1.0,20)": "DSC+"
        })
    elif "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,true,1.0,20)" in cluster_types:
        df["cluster-type"] = df["cluster-type"].replace({
            "DSC(15,100,SSE,CENTROID,-20,20,1.0,0.1,true,1.0,20)": "DSC+"
        })
    df["cluster-type"] = df["cluster-type"].replace({
        "FEACS(10,10,0.5,None,Some(20))": "FEAC-S",
        "CSCS(15,100,None,SSE,STANDARD,Some(1.0),Some(20))": "CSCS"
    })
    return df
cmap = matplotlib.cm.get_cmap('Set2')

styles = {
    "OMRk++": {
        "color": cmap(1),
        "linestyle": "solid",
        "marker": "o",
        "linewidth": 2
    },
    "DSC": {
        "color": cmap(2),
        "linestyle": "dashdot",
        "marker": "o",
        "linewidth": 2
    },
    "DSC+": {
        "color": cmap(0),
        "linestyle": "dashdot",
        "marker": "o",
        "linewidth": 2.5
    },
    "CSCS": {
        "color": cmap(3),
        "linestyle": "dashed",
        "marker": "o",
        "linewidth": 2
    },
    "CSCS1": {
        "color": cmap(3),
        "linestyle": "dashed",
        "marker": "o",
        "linewidth": 2
    },
    "FEAC-S": {
        "color": cmap(4),
        "linestyle": "dotted",
        "marker": "o",
        "linewidth": 2
    },
    "CSCS3": {
        "color": cmap(5),
        "linestyle": "dashed",
        "marker": "o",
        "linewidth": 2
    }
}

def sort_labels_and_handles(labels, handles):
    sorted_labels_and_handles = sorted(zip(labels, handles), key=lambda x: algorithms_order.index(x[0]) if x[0] in algorithms_order else len(algorithms_order))
    return zip(*sorted_labels_and_handles)

def put_lines_on_legend(ax):
    # Adjust the legend, considering the line above the point
    handles, labels = ax.get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
        if label not in newLabels:
            newLabels.append(label)
            correspondance_handle = [h for h, l in zip(handles, labels) if l == label and h != handle]
            if len(correspondance_handle) > 0:
                # Change legend for lines with points
                newHandles.append(ax.plot([], [], linestyle=styles[label]["linestyle"],
                                          color=styles[label]["color"], label=label)[0])# marker=styles[label]["marker"],
            else:
                newHandles.append(handle)
    newLabels, newHandles = sort_labels_and_handles(newLabels, newHandles)
    return newHandles, newLabels

