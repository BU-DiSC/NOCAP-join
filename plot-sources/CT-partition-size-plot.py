from os.path import expanduser
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import numpy as np
import pandas as pd
import os, sys, argparse, copy, time
import math

prop = font_manager.FontProperties(fname="../fonts/LinLibertine_Mah.ttf")
mpl.rcParams['font.family'] = prop.get_name()
mpl.rcParams['text.usetex'] = True
mpl.rcParams.update({'font.size': 30})

PJM_List = ['GHJ','MatrixDP']
#color_List = ["chartreuse4","dodgerblue2","black","brown2","lightgoldenrod4","coral3","dodgerblue4","darkgreen"]
color_List = ["slateblue","#008B45","#CD5B45","#006400"]
marker_list = ['o','x','D']
fillstyles = ['none','none','none']
linestyle_list = ['solid','solid','solid']
label_List = ['GHJ','Optimal','Appr']
markersize_list = [10,10,10,10]

def main(args):
    # Create a dataset:
    interval = 1000
    x_pos = []
    y_list = []
    CT_partition_size_matrix = []
    buffer = int(args.B)
    CT_list = []
    CT_x_list = []
    c1 = math.floor(buffer*4/1.02)
    for PJM in PJM_List:
        CT_partition_size_array = np.genfromtxt('../exp/part-stats-' + PJM + '-'+ args.dist+'-'+ str(buffer) + '.txt', delimiter=",",dtype=float)
        y_list.append([])
        CT_list = []
        CT_x_list = []
        for i in range(CT_partition_size_array.shape[0]):
            if i%interval == 0:
                y_list[-1].append(math.ceil(CT_partition_size_array[i][1]*1.0/c1))
            if i%(interval/10) == 0 and CT_partition_size_array[i][0] != 0:
                CT_list.append(CT_partition_size_array[i][0])
                CT_x_list.append(i*1.0/interval)

        x_pos = list(range(int(CT_partition_size_array.shape[0]/interval)))
        CT_partition_size_array = []





    # plotting
    fig = plt.figure(figsize=(10,5))
    ax = fig.add_subplot(1,1,1)
    for i in range(len(PJM_List)):
        tmp_x_pos = x_pos.copy()
        tmp_y_list = y_list[i].copy()
        j = 0
        while j < len(tmp_y_list):
            if tmp_y_list[j] == 0:
                del tmp_x_pos[j]
                del tmp_y_list[j]
            else:
                j += 1

        x_mark_pos = []
        tick_interval = 50
        for j in range(len(tmp_x_pos)):
            if j%tick_interval == 0:
                x_mark_pos.append(j)
        #ax.plot(x_pos, y_list[i],'b',linewidth=2,marker='x',markevery=markers_on, label=label_List[i])
        ax.plot(tmp_x_pos, tmp_y_list,color=color_List[i],linewidth=2,fillstyle=fillstyles[i],marker=marker_list[i],markersize=markersize_list[i], markevery=x_mark_pos,label=label_List[i])


    #plt.ylim(ymin=0, ymax=250)
    #plt.xlim(xmin=0)

    x_tick_texts = []
    x_new_pos = []
    tick_interval = 100
    for i in range(len(x_pos)+1):
        if i%tick_interval == 0:
            x_new_pos.append(i)
            x_tick_texts.append(str(int(i*interval/100000)))
    ax.set_xticks(x_new_pos)
    ax.set_xticklabels(x_tick_texts)

    ax.set_xlabel('CT-sorted record idx ' + r'$(\times 10^5)$',fontsize=30)
    ax.legend(loc='upper center',ncol=2, fontsize=24, columnspacing=0.6, labelspacing=0.2, handletextpad=0.5,borderpad=0.3, handlelength=1.3)

    ax.set_ylim(ymin=0)
    ax.set_yticks([0,2,4,6])
    ax.set_ylabel('\#passes '+ r'$\left(\left\lceil \frac{\|\mathcal{N}(i)\|}{c_R} \right\rceil\right)$',fontsize=30)


    ax2 = ax.twinx()
    ax2.plot(CT_x_list, CT_list,'b:',linewidth=3,label="CT Value")
    ax2.set_ylabel('CT Value',fontsize=30)
    ax2.set_yscale('log')
    ax2.set_ylim(ymin=0.1,ymax=10000)
    ax2.set_yticks([0.1, 1,10,100,1000,10000])
    ax2.legend(loc='upper center',bbox_to_anchor=(0.5, 0.8), handletextpad=0.5,borderpad=0.3,fontsize=24)
    #ax2.set_yticklabels(["0$\%$","10$\%$","20$\%$","30$\%$","40$\%$","50$\%$"])


    plt.tight_layout()
    fig.savefig("../sorted-ct-idx-partition-size-"+ args.name + "-" + str(buffer) + ".pdf",bbox_inches = "tight", dpi=900)



    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-buffer-size-emul-plot')
    parser.add_argument('--dist',help='suffix to be plot',type=str, default='uni')
    parser.add_argument('--name',help='name of the plot', default='uniform-distribution')
    parser.add_argument('--B',help='buffer value', default=320)
    args = parser.parse_args()
    main(args)
