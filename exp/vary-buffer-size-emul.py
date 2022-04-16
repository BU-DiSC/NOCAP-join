import os, sys, argparse, copy, time

#B_List = [256, 512, 1024, 2048, 4096, 8192]
B_List = list(range(128, 512+1, 32))
#B_List = list(range(288, 352+1, 8))
#B_List.reverse()
#B_List = list(range(256, 512+1, 32))
#PJM_List = ['Hash','MatrixDP','DHH --num_parts=32','DHH --num_parts=64','DHH --num_parts=128']
PJM_List = ['GHJ --mu 1 --tau 1 --NoDirectIO --NoJoinOutput', 'ApprMatrixDP --RoundedHash --mu 1 --tau 1 --NoDirectIO --NoJoinOutput', 'SMJ --NoDirectIO --NoJoinOutput', 'MatrixDP --mu 1 --tau 1 --NoDirectIO --NoJoinOutput']
#PJM_List = ['GHJ', 'ApprMatrixDP --RoundedHash', 'ApprMatrixDP', 'GHJ --RoundedHash', 'GHJ --mu 1 --tau 1', 'SMJ']
metric_mapping = {
        'Join Time':['total',-2], 
        'Output #entries':['output_entries',-1], 
        'Read #pages:':['read_pages_tt',-1],
        'Write #pages':['write_pages_tt',-1],
        'I/O Time':['io',-2],
        'Partition Time':['partition',-2],
        'Probe Time':['probe',-2],
        'Read Latency':['Read_latency',-2],
        'Write Latency':['Write_latency',-2],
        'algo Time':['algo_latency',-2],
        }
def parse_output(filename):
    f = open(filename, 'r')
    tmp_data = f.readlines()
    tt = {}
    for line in tmp_data:
        x = line.strip().split(" ")
        for key in metric_mapping:
            if key in line:
                if metric_mapping[key][1] == -1:
                    tt[metric_mapping[key][0]] = int(x[-1])
                else:
                    tt[metric_mapping[key][0]] = float(x[-2])
    f.close()
    return tt 

def merge(result1, result2):
    if result1 == {}:
        return copy.deepcopy(result2)
    result = copy.deepcopy(result1)
    error = False
    for key in result2:
        if key not in result:
            error = True
            break
    if error:
        return result
    for key in result2:
        result[key] += result2[key]
    return result



def output(result, filename):
    f = open(filename,'w')
    f.write('buffer')
    for pjm in PJM_List:
        s = ""
        for key in metric_mapping:
            s += ',' + metric_mapping[key][0] + '-' + pjm
            
        f.write(s)
    f.write('\n')
    for i, B in enumerate(B_List):
        f.write(str(B))
        for j in range(len(PJM_List)):
            error = False
            for _, v in metric_mapping.items():
                if v[0] not in result[i][j]:
                    error = True
                    break
            if error:
                continue

            for _, v in metric_mapping.items():
                f.write(','+str(result[i][j][v[0]]))
        f.write('\n')
    f.close()



def main(args):
    result = [[{} for pjm in PJM_List] for i in range(len(B_List))]
    for k in range(args.tries):
        cmd = '../build/load-gen ' + ' --lE ' + str(args.lE) + ' --rE ' + str(args.rE) + ' --lTS ' + str(args.lTS) + ' --rTS ' + str(args.rTS) + ' -K ' + str(args.K) + ' --JD ' + str(args.JD) + ' --JD_NDEV ' + str(args.JD_NDEV) + ' --JD_ZALPHA ' + str(args.JD_ZALPHA)
        print(cmd)
        os.system(cmd)
        '''
        if k == 0:
            os.system("sed '1d' workload-dis.txt | awk -F' ' '{print $2}' > workload-JD-" + str(args.JD) + '-JD_NDEV-' + str(args.JD_NDEV) + '-JD_ZALPHA-' + str(args.JD_ZALPHA) + '.txt')
        '''
        os.system("sync")
        time.sleep(2)
        for i,B in enumerate(B_List,start=0):
            for j, pjm in enumerate(PJM_List, start=0):
                #print("Running " + pjm)
                print('../build/emul ' + '-B ' + str(B) + ' --PJM-' + pjm)
                os.system('../build/emul ' + '-B ' + str(B) + ' --PJM-' + pjm + ' > output.txt')
                tmp = parse_output('output.txt')
                print("Finish " + pjm + " with cost time: " + str(tmp['total']))
                print("I/O cnt: " + str(tmp['read_pages_tt'] + tmp['write_pages_tt']))
                print("Output #entries: " + str(tmp['output_entries']))
                os.system('rm output.txt')
                result[i][j] = merge(result[i][j], tmp)
        os.system('rm workload-rel-R.dat')
        os.system('rm workload-rel-S.dat')
    for i in range(len(B_List)):
        for j in range(len(PJM_List)):
            for k in result[i][j]:
                result[i][j][k] /= args.tries*1.0
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
    parser.add_argument('--OP',help='the path to output the result', default="emul-vary_buffer_size.txt", type=str)

    parser.add_argument('--IO',help='the IO latency in microseconds per I/O [def: 100 us]', default=100, type=float)

    args = parser.parse_args()
    main(args)

