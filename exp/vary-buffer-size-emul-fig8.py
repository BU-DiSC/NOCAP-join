import os, sys, argparse, copy, time

# Intro Exp:
B_List = [128+x*128 for x in range(0, 10)] + [2000 + 10000*x for x in range(0, 26)]
# Exp 1
#B_List = [int(2**(x/2+8)) if x%2 == 0 else int((2**(x//2 + 8) + 2**(x//2 + 7))) for x in range(21)]
# Exp 3
# B_List = range(128, 512+32, 32)

# Intro Exp
#shared_params = " --NoJoinOutput --mu 1 --tau 1 --NoDirectIO --NoSyncIO "
# Exp 1/3
shared_params = " --NoJoinOutput --mu 1.5 --tau 1.43 --NoSyncIO "
# Exp 2
#shared_params = " --NoJoinOutput " # sync I/O on (default)



# Intro Exp
PJM_List = [ 'DHH  --DHH_skew_frac_threshold=0.0', 'HybridApprMatrixDP --RoundedHash', 'HybridMatrixDP --RoundedHash']
# Exp 1/3
PJM_List = ['GHJ','SMJ','DHH', 'HybridApprMatrixDP --RoundedHash', 'HybridMatrixDP --NoDirectIO --NoSyncIO --RoundedHash', 'DHH --DHH_skew_frac_threshold=0.0']
# Exp 2


metric_mapping = {
        'Join Time':['total',-2], 
        'Output #entries':['output_entries',-1], 
        'Total Read #pages:':['read_pages_tt',-1],
        'Total Write #pages':['write_pages_tt',-1],
        'Sequential Write #pages':['seq_write_pages_tt',-1],
        'Normalized I/Os':['normalized_io_tt',-1],
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
                    tt[metric_mapping[key][0]] = int(float(x[-1]))
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
    print("Running buffer list:")
    print(B_List)
    result = [[{} for pjm in PJM_List] for i in range(len(B_List))]
    path_str = ' --path-dis="' + str(args.DataDir) + '/workload-dis.txt" --path-rel-R="' + str(args.DataDir) + '/workload-rel-R.dat" --path-rel-S="' + str(args.DataDir) + '/workload-rel-S.dat" '
    for k in range(args.tries):
        cmd = '../build/load-gen ' + ' --lE ' + str(args.lE) + ' --rE ' + str(args.rE) + ' --lTS ' + str(int(args.lTS)) + ' --rTS ' + str(args.rTS) + ' --join-key-size ' + str(args.K) + ' --JD ' + str(args.JD) + ' --JD_NDEV ' + str(args.JD_NDEV) + ' --JD_ZALPHA ' + str(args.JD_ZALPHA) + path_str
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
                print('../build/emul ' + '-B ' + str(B) + ' --PJM-' + pjm + shared_params + ' -k ' + str(args.k) + path_str)
                os.system('../build/emul ' + '-B ' + str(B) + ' --PJM-' + pjm + shared_params +  ' -k ' + str(args.k) + path_str + ' > output.txt')
                tmp = parse_output('output.txt')
                print("Finish " + pjm + " with cost time: " + str(tmp['total']))
                print("Normalized I/O cnt: " + str(tmp['normalized_io_tt']))
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
    parser.add_argument('--tries',help='the number of tries', default=3, type=int)
    parser.add_argument('--lTS',help='the number of entries in the left table (to be partitioned first)', default=1000000, type=float)
    parser.add_argument('--rTS',help='the number of entries in the right table', default=8000000, type=int)
    parser.add_argument('--lE',help='the entry size in the left table', default=1024, type=int)
    parser.add_argument('--rE',help='the entry size in the right table', default=1024, type=int)
    parser.add_argument('-K',help='the key size', default=8, type=int)
    parser.add_argument('--DataDir',help='the working data directory', default="./", type=str)
    parser.add_argument('--JD',help='the distribution of the right table [0: uniform, 1:normal, 2: beta, 3:zipf]', default=0, type=int)
    parser.add_argument('-k',help='the number of the most frequent-matching keys to be tracked', default=50000, type=int)
    parser.add_argument('--JD_NDEV',help='the standard deviation of the normal distribution if specified', default=1.0, type=float)
    parser.add_argument('--JD_ZALPHA',help='the alpha value of the zipfian distribution if specified', default=1.0, type=float)
    parser.add_argument('--OP',help='the path to output the result', default="emul-vary_buffer_size.txt", type=str)

    parser.add_argument('--IO',help='the IO latency in microseconds per I/O [def: 100 us]', default=100, type=float)

    args = parser.parse_args()
    main(args)

