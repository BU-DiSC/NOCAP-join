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
mpl.rcParams.update({'font.size': 26})


label_List = ['NOCAP','DHH','Histojoin']

GLOBAL_PJM_List = ['HybridApprMatrixDP --RoundedHash','DHH','SMJ','GHJ']
#color_List = ["chartreuse4","dodgerblue2","black","brown2","lightgoldenrod4","coral3","dodgerblue4","darkgreen"]
color_List = ["#FF6103","#008B45","#9932CC","slateblue","#CD5B45"]
marker_list = ['s','x','^','o','*']
fillstyles = ['none','none','none','none','none','none']
linestyle_list = ['solid','solid','solid','solid','dashed']
label_List = ['NOCAP','DHH','SMJ','GHJ','OCAP']
markersize_list = [10,10,10,10,10,10]


def main(args):
    # Create a dataset:
    if args.noPrefix:
        path = '../exp/' + args.suffix + '.txt'
    else:
        path = '../exp/emul_vary_buffer_size_' + args.suffix + '.txt'
    df = pd.read_csv(path)
    x_pos = df['buffer'].tolist()
    y_list = []

    
        

    if not args.io and not args.tput:
        PJM_List = GLOBAL_PJM_List[:-1]
    else:
        PJM_List = GLOBAL_PJM_List
    
    if args.OnlyNOCAPAndDHH:
        PJM_List = PJM_List[:3]
    for PJM in PJM_List:
        if args.tput:
            y_list.append([args.input*1000/x for x in df['total-'+PJM].tolist()])
        elif args.io:
            tmp1 = df['write_pages_tt-'+PJM].tolist()
            tmp2 = df['read_pages_tt-'+PJM].tolist()
            y_list.append([])
            for i in range(len(tmp1)):
                y_list[-1].append(tmp1[i] + tmp2[i])
        else:
            if args.ymax <= 1.0:
                y_list.append([x for x in df['total-'+PJM].tolist()])
            else:
                y_list.append([x/60.0 for x in df['total-'+PJM].tolist()])

    # plotting
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    x = np.linspace(x_pos[0], x_pos[-1], 100)
    for i in range(len(PJM_List)):
        markers_on = []
        for j in range(len(y_list[i])):
            if j%3 == 0 or args.plainXaxis:
                markers_on.append(j)
        if markers_on[-1] != len(y_list[i]) - 1:
            markers_on.append(len(y_list[i]) - 1)
        if args.smooth:
            fun = interp1d(x=x_pos, y=y_list[i], kind=2)
            y_smooth_list = [fun(i) for i in x]
            ax.plot(x, y_smooth_list,color=color_List[i],linewidth=2,fillstyle=fillstyles[i], label=label_List[i])
        else:
        #ax.plot(x_pos, y_list[i],'b',linewidth=2,marker='x',markevery=markers_on, label=label_List[i])
            ax.plot(x_pos, y_list[i],color=color_List[i],linewidth=2,fillstyle=fillstyles[i],marker=marker_list[i],markevery=markers_on, markersize=markersize_list[i], label=label_List[i])

    #plt.ylim(ymin=0, ymax=250)
    #plt.xlim(xmin=0)
    x_tick_texts = []
    x_axis_tick_pos = []
    if args.plainXaxis:
        for i in range(len(x_pos)):
            if i%2 == 0:
                x_axis_tick_pos.append(x_pos[i])
                x_tick_texts.append(str(x_pos[i]))
        plt.xticks(x_axis_tick_pos, x_tick_texts)
        if not args.NoXaxisTitle:
            plt.xlabel('Buffer size (pages)',fontsize=30)
    else:
        for i in range(len(x_pos)):
            if i%4 == 0:
                x_axis_tick_pos.append(x_pos[i])
                x_tick_texts.append("$2^{"+str(int(math.log(x_pos[i], 2)))+"}$")
        ax.set_xscale('log')
        plt.minorticks_off()
        plt.xticks(x_axis_tick_pos, x_tick_texts)
        if not args.NoXaxisTitle:
            plt.xlabel('Buffer size (pages) [log scale]',fontsize=30)

    if args.showLegend:
        legend=ax.legend(loc=args.legendLocation,ncol=int(math.ceil(len(PJM_List)*1.0/3)),fontsize=20, columnspacing=0.5, labelspacing=0.2, handletextpad=0.4,borderaxespad=0.3, handlelength=1.2,framealpha=0.5)
    #legend.get_frame().set_alpha(None)
    #legend.get_frame().set_facecolor((0, 0, 1, 0.1))
    if args.noPrefix:
        output_path = "../"+ args.name
    else:
        output_path = "../vary-buffer-size-emul-"+ args.name

    if args.tput:
        plt.ylim(ymin=0)
        plt.ylabel('Join Tput (k/s)',fontsize=30)
        plt.tight_layout()
        output_path += "-tput"
    elif args.io:
        plt.ylim(ymin=0,ymax=1.5e7)
        plt.yticks([x*0.3e7 for x in range(6)])
        plt.ylabel('\# I/Os',fontsize=30)
        plt.tight_layout()
        output_path += "-io"
    else:
        if args.ymax > 5:
            plt.ylim(ymin=0,ymax=args.ymax)
            plt.yticks([x*2 for x in range(int(args.ymax/2)+1)], ['%.1f' % (x*2) for x in range(int(args.ymax/2)+1)])
            plt.ylabel('Latency (min)',fontsize=30)
        elif args.ymax > 1:
            plt.ylim(ymin=0,ymax=args.ymax)
            plt.yticks([x for x in range(int(args.ymax)+1)], ['%.1f' %x for x in range(int(args.ymax)+1)])
            plt.ylabel('Latency (min)',fontsize=30)
        else:
            plt.ylim(ymin=0,ymax=round(60*args.ymax))
            plt.yticks([x*10 for x in range(int(round(60*args.ymax)/10+1))])
            plt.ylabel('Latency (s)',fontsize=30)
        plt.tight_layout()
        output_path += "-lat"

    if args.smooth:
        output_path += "-smooth"
    if args.png:
        output_path += ".png"
    else:
        output_path += ".pdf"
    fig.savefig(output_path,bbox_inches = "tight", dpi=900)


    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-buffer-size-emul-plot')
    parser.add_argument('--suffix',help='suffix to be plot',type=str, default='256-262144-mu-2.9-tau-2.1-nosyncio')
    parser.add_argument('--name',help='name of the plot', default='256-262144-mu-1.5-tau-1.43-nosyncio')
    parser.add_argument('--input',help='the input size (unit: million)', default=9, type=int)
    parser.add_argument('--io', action='store_true', help='using I/O as the metric')
    parser.add_argument('--OnlyNOCAPAndDHH', action='store_true', help='only select NOCAP and DHH to plot')
    parser.add_argument('--smooth', action='store_true', help='making curve smoother')
    parser.add_argument('--showLegend', action='store_true', help='show the legend')
    parser.add_argument('--plainXaxis', action='store_true', help='disable log scale x-axis')
    parser.add_argument('--NoXaxisTitle', action='store_true', help='disable x-axis title')
    parser.add_argument('--noPrefix', action='store_true', help='disable the prefix of the data path and the output path')
    parser.add_argument('--ymax', help='maximum y value', default=4, type=float)
    parser.add_argument('--tput', action='store_true', help='using throughput as the metric')
    parser.add_argument('--legendLocation', help='location of legend', default="upper right")
    parser.add_argument('--png', action='store_true', help='produce png')
    args = parser.parse_args()
    main(args)
