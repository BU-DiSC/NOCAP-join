from os.path import expanduser
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d

import matplotlib.font_manager as font_manager
import numpy as np
import pandas as pd
import os, sys, argparse, copy, time, math

prop = font_manager.FontProperties(fname="../fonts/LinLibertine_Mah.ttf")
mpl.rcParams['font.family'] = prop.get_name()
mpl.rcParams['text.usetex'] = True
mpl.rcParams.update({'font.size': 28})

GLOBAL_PJM_List = ['HybridApprMatrixDP --RoundedHash','DHH']
#color_List = ["dodgerblue2","brown2","lightgoldenrod4","coral3","dodgerblue4","darkgreen"]
color_List = ["#FF6103","#008B45","#CD5B45","#006400"]
marker_list = ['s','x','*','.']
fillstyles = ['none','none','none']
linestyle_list = ['solid','solid','dashed']
linestyle2_list = ['dotted','dotted','dashed']
label_List = ['NOCAP','DHH','OCAP']
markersize_list = [10,10,10,8,8]


def main(args):
    # Create a dataset:
    path = '../exp/' + args.suffix + '.txt'

    df = pd.read_csv(path)
    x_pos = df['buffer'].tolist()
    y_list = []
    y_io_lat_list = []
    PJM_List = GLOBAL_PJM_List


    for PJM in PJM_List:
        #print(PJM)
        if args.ymax <= 1.0:
            y_list.append([x for x in df['total-'+PJM].tolist()])
            y_io_lat_list.append([x for x in df['io-'+PJM].tolist()])
        else:
            y_list.append([x/60.0 for x in df['total-'+PJM].tolist()])
            y_io_lat_list.append([x/60.0 for x in df['io-'+PJM].tolist()])
        #print(y_list)

    # plotting
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    x = np.linspace(x_pos[0], x_pos[-1], 100)
    for i in range(len(PJM_List)):
        markers_on = []
        for j in range(len(y_list[i])):
            if j%2 == 0:
                markers_on.append(j)
        #ax.plot(x_pos, y_list[i],'b',linewidth=2,marker='x',markevery=markers_on, label=label_List[i])
        ax.plot(x_pos, y_list[i],linestyle=linestyle_list[i], color=color_List[i],linewidth=2,fillstyle=fillstyles[i],marker=marker_list[i], markersize=markersize_list[i],markevery=markers_on, label=label_List[i])
        ax.plot(x_pos, y_io_lat_list[i],linestyle=linestyle2_list[i],color=color_List[i],linewidth=2,fillstyle=fillstyles[i],marker=marker_list[i], markevery=markers_on,markersize=markersize_list[i])

    #plt.ylim(ymin=0, ymax=250)
    #plt.xlim(xmin=0)
    x_tick_texts = []
    x_axis_tick_pos = []

    for i in range(len(x_pos)):
        if i%2 == 0:
            x_axis_tick_pos.append(x_pos[i])
            x_tick_texts.append(str(int(x_pos[i]*4/1024)))
    ax.set_xscale('log')
    plt.minorticks_off()
    plt.xticks(x_axis_tick_pos, x_tick_texts)
    plt.xlabel('Buffer size (MB) [log scale]',fontsize=30)

    if args.showLegend:
        legend=ax.legend(loc=args.legendLocation,ncol=int(len(PJM_List)/2),fontsize=20, columnspacing=0.5, labelspacing=0.2, handletextpad=0.4,borderaxespad=0.3, handlelength=1.2,framealpha=0.5)
        legend2 = ax.legend(['total', 'io'], ncol=1, loc='lower right',fontsize=20, columnspacing=0.5, labelspacing=0.2, handletextpad=0.4,borderaxespad=0.3, handlelength=1.2,framealpha=0.5)
        ax.add_artist(legend)
        ax.add_artist(legend2)

    #legend.get_frame().set_alpha(None)
    #legend.get_frame().set_facecolor((0, 0, 1, 0.1))
    output_path = "../"+ args.name


    if args.ymax > 1:
        plt.ylim(ymin=0,ymax=args.ymax)
        if args.ymax < 3:
            plt.yticks([x*0.5 for x in range(int(args.ymax)*2+1)])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax < 6:
            plt.yticks([x for x in range(int(args.ymax)+1)])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax < 16:
            plt.yticks([x*2 for x in range(int(math.ceil(args.ymax/2)+1))])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax <= 35:
            plt.yticks([x*5 for x in range(int(math.ceil(args.ymax/5)+1))])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax < 71:
            plt.yticks([x*10 for x in range(int(math.ceil(args.ymax/10)+1))])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax < 90:
            plt.yticks([x*15 for x in range(int(math.ceil(args.ymax/15)+1))])
            plt.ylabel('Latency (min)',fontsize=30)
        else:
            plt.yticks([x*60 for x in range(int(math.ceil(args.ymax/60)+1))],[x for x in range(int(math.ceil(args.ymax/60)+1))])
            plt.ylabel('Latency (hour)',fontsize=30)
        
    else:
        plt.ylim(ymin=0,ymax=round(60*args.ymax))
        plt.yticks([x*10 for x in range(int(round(60*args.ymax)/10+1))])
        plt.ylabel('Latency (s)',fontsize=30)
    plt.tight_layout()
    output_path += "-lat"

    if args.png:
        output_path += ".png"
    else:
        output_path += ".pdf"
    fig.savefig(output_path,bbox_inches = "tight", dpi=900)


    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-buffer-size-emul-plot')
    parser.add_argument('--suffix',help='suffix to be plot',type=str, default='job-exp-emul-nosyncio')
    parser.add_argument('--name',help='name of the plot', default='job-exp-mu-1.2-tau-1.14-nosyncio')
    parser.add_argument('--input',help='the input size (unit: million)', default=9, type=int)
    parser.add_argument('--showLegend', action='store_true', help='show the legend')
    parser.add_argument('--png', action='store_true', help='produce png')
    parser.add_argument('--ymax', help='maximum y value', default=4, type=float)
    parser.add_argument('--legendLocation', help='location of legend', default="upper right")
    args = parser.parse_args()
    main(args)
