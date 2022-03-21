import os, sys, argparse

#B_List = [128, 256, 512, 1024, 2048, 4096, 8192]
B_List = list(range(480, 512+1, 4))
#B_List = list(range(256, 512+1, 32))
PJM_List = ['Hash','BNLJ','Hash -H', 'MatrixDP', 'Hybrid']

def parse_output(filename):
    f = open(filename, 'r')
    tmp_data = f.readlines()
    repartition_time = 0
    for line in tmp_data:
        x = line.strip().split(" ")
        if "Total repartition cost" in line:
            #print(x)
            repartition_time =  float(x[-2])/1000000.0/60.0
            break
    data = tmp_data[-4:]
    IOs = int(data[0].strip())
    tmp = data[1].strip().split(' ')
    cpu_time = float(tmp[-2])/1000000.0/60.0
    tmp = data[2].strip().split(' ')
    io_time = float(tmp[-2])/1000000.0/60.0
    f.close()
    return cpu_time, io_time, repartition_time


def output(result, filename):
    f = open(filename,'w')
    f.write('buffer')
    for pjm in PJM_List:
        f.write(',cpu-' + pjm + ',io-' + pjm+',repar-io-'+pjm)
    f.write('\n')
    for i, B in enumerate(B_List):
        f.write(str(B))
        for j in range(len(PJM_List)):
            f.write(','+str(result[i][j][0])+','+str(result[i][j][1]) + ','+str(result[i][j][2]))
        f.write('\n')
    f.close()



def main(args):
    result = [[[0,0,0] for pjm in PJM_List] for i in range(len(B_List))]
    for k in range(args.tries):
        os.system('../build/load-gen ' + ' --lE ' + str(args.lE) + ' --rE ' + str(args.rE) + '--lTS ' + str(args.lTS) + ' --rTS ' + str(args.rTS) + ' -K ' + str(args.K) + ' --JD ' + str(args.JD) + ' --JD_NDEV ' + str(args.JD_NDEV) + ' --JD_ZALPHA ' + str(args.JD_ZALPHA))
        if k == 0:
            os.system("sed '1d' workload.txt | awk -F' ' '{print $2}' > workload-JD-" + str(args.JD) + '-JD_NDEV-' + str(args.JD_NDEV) + '-JD_ZALPHA-' + str(args.JD_ZALPHA) + '.txt')
        for i,B in enumerate(B_List,start=0):
            for j, pjm in enumerate(PJM_List, start=0):
                os.system('../build/estm ' + '-B ' + str(B) + ' --IO ' + str(args.IO)  + ' --PJM-' + pjm + ' > output.txt')
                tmp = parse_output('output.txt')
                os.system('rm output.txt')
                result[i][j][0] += tmp[0]
                result[i][j][1] += tmp[1]
                result[i][j][2] += tmp[2]
        os.system('rm workload.txt')
    for i in range(len(B_List)):
        for j in range(len(PJM_List)):
            result[i][j][0] /= args.tries*1.0
            result[i][j][1] /= args.tries*1.0
            result[i][j][2] /= args.tries*1.0
    output(result, args.OP)

if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-buffer-size-exp')
    parser.add_argument('--tries',help='the number of tries', default=1, type=int)
    parser.add_argument('--lTS',help='the number of entries in the left table (to be partitioned first)', default=1000000, type=int)
    parser.add_argument('--rTS',help='the number of entries in the right table', default=8000000, type=int)
    parser.add_argument('--lE',help='the entry size in the left table', default=1024, type=int)
    parser.add_argument('--rE',help='the entry size in the right table', default=1024, type=int)
    parser.add_argument('-K',help='the key size', default=8, type=int)
    parser.add_argument('--JD',help='the distribution of the right table [0: uniform, 1:normal, 2: beta, 3:zipf]', default=0, type=int)
    parser.add_argument('--JD_NDEV',help='the standard deviation of the normal distribution if specified', default=1.0, type=float)
    parser.add_argument('--JD_ZALPHA',help='the alpha value of the zipfian distribution if specified', default=1.0, type=float)
    parser.add_argument('--OP',help='the path to output the result', default="vary_buffer_size.txt", type=str)

    parser.add_argument('--IO',help='the IO latency in microseconds per I/O [def: 100 us]', default=100, type=float)

    args = parser.parse_args()
    main(args)

