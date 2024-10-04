import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import argparse
from scipy.interpolate import make_interp_spline, BSpline
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from os.path import expanduser
import math

prop = font_manager.FontProperties(fname="../fonts/LinLibertine_Mah.ttf")
mpl.rcParams['font.family'] = prop.get_name()
mpl.rcParams['text.usetex'] = True
mpl.rcParams.update({'font.size': 28})


mem_th_list = [0.0,0.01,0.02,0.03,0.04]
freq_th_list = [0.04,0.03,0.02,0.01,0.0]
mem_th_list = [0.0,0.02,0.04,0.06,0.08]
freq_th_list = [0.12,0.09,0.06,0.03,0.0]

def main(args):
    suffix = ".txt"

    path = "../exp/" + args.suffix + suffix
    df = pd.read_csv(path)
    buff_index = df['buffer'].tolist().index(args.buff)
    if args.io:
        NOCAP_value = df['normalized_io_tt-HybridApprMatrixDP --RoundedHash'].tolist()[buff_index]
    else:
        NOCAP_value = df['total-HybridApprMatrixDP --RoundedHash'].tolist()[buff_index]
    data = np.zeros((len(mem_th_list),len(freq_th_list)))
    raw_data = np.zeros((len(mem_th_list),len(freq_th_list)))
    for i, mem_th in enumerate(mem_th_list):
        prev_io_value = 0
        stored_value = []
        io_value = 0
        for j, freq_th in enumerate(freq_th_list):
            freq_th_str = str(freq_th)
            if mem_th == 0:
                freq_th_str = '0.00'
            io_value = df['normalized_io_tt-DHH --DHH_skew_frac_threshold=' + freq_th_str + ' --DHH_skew_mem_percent=' + str(mem_th)].tolist()[buff_index]
            print(io_value)
            if args.io:
                value = io_value
            else:
                value = df['total-DHH --DHH_skew_frac_threshold=' + freq_th_str + ' --DHH_skew_mem_percent=' + str(mem_th)].tolist()[buff_index]
            '''
            if prev_io_value == io_value:
                stored_value.append(value)
            else:
                if j > 0:
                    avg_value = sum(stored_value)/len(stored_value)
                    for k in range(len(stored_value)):
                        data[i][j-k-1] = avg_value/NOCAP_value
                stored_value = [value]
            prev_io_value = io_value
            '''
            data[i][j] = value*1.0/NOCAP_value - 1

        #avg_value = sum(stored_value)/len(stored_value)
        #for k in range(len(stored_value)):
        #    data[i][len(freq_th_list)-k-1] = avg_value

    print(data)
    data = data.T
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)

    #colors = ["darkblue","lightcoral","red"]
    colors = ["black","blue","white"]
    cmap1 = LinearSegmentedColormap.from_list("mycmap", colors)
    plt.imshow(data,cmap=cmap1,vmin=0.0,vmax=0.26)

    meta_font_size = 28
    plt.xlabel(r'mem of hash table',fontsize=meta_font_size-2)
    plt.xticks([i for i in range(len(mem_th_list))],[str(round(x*100))+'\%' for x in mem_th_list], fontsize=meta_font_size-8)
    plt.ylabel(r'freq threshold',fontsize=meta_font_size-2)
    plt.yticks([i for i in range(len(freq_th_list))],[str(round(x*100))+'\%' for x in freq_th_list], fontsize=meta_font_size-8)
    # if baseline == 'GHJ':
    #     plt.title('(a) ADMP v.s. GHJ', fontsize=meta_font_size)
    # else:
    #     plt.title('(b) ADMP v.s. SMJ', fontsize=meta_font_size)
    cbar = plt.colorbar(ticks=[0.0,0.05,0.1,0.15,0.2,0.25])
    cbar.ax.set_yticklabels(['0\%','5\%','10\%','15\%','20\%','25\%'])
    plt.tight_layout()
    
    output_path = "../vary-DHH-thresholds-emul-B-"+ str(args.buff) + "-"+ args.name
    if args.png:
        output_path += ".png"
    else:
        output_path += ".pdf"
    fig.savefig(output_path,bbox_inches = "tight", dpi=900)


    plt.show()



if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-lTS-emul-plot')
    parser.add_argument('--suffix',help='suffix to be plot',type=str, default='')
    parser.add_argument('--name',help='name of the plot', default='zipfian-distribution')
    parser.add_argument('--buff',help='buffer size', type=int, default=379)
    parser.add_argument('--io',action='store_true', help='using io as metrics')
    parser.add_argument('--png', action='store_true', help='produce png')
    args = parser.parse_args()
    main(args)
