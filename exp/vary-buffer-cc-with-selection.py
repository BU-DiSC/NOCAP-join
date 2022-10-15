import os, sys, argparse, copy, time, random, threading

B_List=[126]
shared_params = " --NoJoinOutput --NoSyncIO"
PJM_List = ['ApprMatrixDP --RoundedHash --mu 2.4 --tau 2.2', 'GHJ --mu 2.4 --tau 2.2']
result = None
lock = threading.Lock()
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
    tmp_result = copy.deepcopy(result1)
    error = False
    for key in result2:
        if key not in tmp_result:
            error = True
            break
    if error:
        return tmp_result
    for key in result2:
        tmp_result[key] += result2[key]
    return tmp_result

def output(final_result, filename):
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
                if v[0] not in final_result[i][j]:
                    error = True
                    break
            if error:
                continue

            for _, v in metric_mapping.items():
                f.write(','+str(final_result[i][j][v[0]]))
        f.write('\n')
    f.close()


def run(args, thread_idx, i, B, j, pjm, lSR, rSR):
    global result
    output_path = args.TMP_OUT_DIR + '/output' + str(thread_idx) + '.txt'
    os.system('mkdir cc-exp-' + str(thread_idx))
    #prin-("Running " + pjm)
    cmd = '../../build/emul' + ' --lSR ' + str(args.lSR) + ' --rSR ' + str(args.rSR) + ' --lSS ' + str(lSR) + ' --rSS ' + str(rSR) + ' -B ' + str(B) + ' --PJM-' + pjm + shared_params +  ' -k ' + str(args.k) + ' --path-dis="../workload-dis.txt" --path-rel-R="../workload-rel-R.dat" --path-rel-S="../workload-rel-S.dat"'+ ' > ' + output_path
    print(cmd)
    os.system('cd cc-exp-' + str(thread_idx) + ';' + cmd + '; cd ../')
    tmp = parse_output(output_path)
    os.system('rm ' + output_path)
    result[thread_idx][i][j] = merge(result[thread_idx][i][j], tmp)
    #os.system('rm -rf cc-exp-' + str(thread_idx))

def main(args):
    global result
    result = [[[{} for pjm in PJM_List] for i in range(len(B_List))] for y in range(args.threads)]
    left_seeds = [[] for k in range(args.tries)]
    right_seeds = [[] for k in range(args.tries)]
    for k in range(args.tries):
        for y in range(args.threads):
            left_seeds[k].append(random.getrandbits(32))
            right_seeds[k].append(random.getrandbits(32))
    acc_result = [[{} for pjm in PJM_List] for i in range(len(B_List))] 
    for k in range(args.tries):
        cmd = '../build/load-gen ' + ' --lE ' + str(args.lE) + ' --rE ' + str(args.rE) + ' --lTS ' + str(int(args.lTS)) + ' --rTS ' + str(args.rTS) + ' -K ' + str(args.K) + ' --JD ' + str(args.JD) + ' --JD_NDEV ' + str(args.JD_NDEV) + ' --JD_ZALPHA ' + str(args.JD_ZALPHA)
        print(cmd)
        os.system(cmd)
        os.system("sync")
        time.sleep(2)
        for i,B in enumerate(B_List,start=0):
            for j, pjm in enumerate(PJM_List, start=0):
                for t in range(args.threads):
                    os.system('rm -rf cc-exp-' + str(t))
                thread_list = [threading.Thread(target=run, args=(args, x, i, B, j, pjm, left_seeds[k][x], right_seeds[k][x])) for x in range(args.threads)]
                for t in thread_list:
                    t.start()
                for t in thread_list:
                    t.join()

        
        os.system('rm workload-rel-R.dat')
        os.system('rm workload-rel-S.dat')
        
    for i in range(len(B_List)):
        for j in range(len(PJM_List)):
            for x in range(args.threads):
                acc_result[i][j] = merge(acc_result[i][j], result[x][i][j])
            for k in acc_result[i][j]:
                acc_result[i][j][k] /= args.tries*args.threads*1.0
    output(acc_result, args.OP)
    for t in range(args.threads):
        os.system('rm -rf cc-exp-' + str(t))

if __name__ == "__main__":
    parser = argparse.ArgumentParser('vary-buffer-cc-with-selection-exp')
    parser.add_argument('--tries',help='the number of tries', default=3, type=int)
    parser.add_argument('--lTS',help='the number of entries in the left table (to be partitioned first)', default=5000000, type=float)
    parser.add_argument('--rTS',help='the number of entries in the right table', default=40000000, type=int)
    parser.add_argument('--lE',help='the entry size in the left table', default=1024, type=int)
    parser.add_argument('--rE',help='the entry size in the right table', default=1024, type=int)
    parser.add_argument('--lSR',help='the selection ratio in the left table', default=0.8, type=float)
    parser.add_argument('--rSR',help='the selection ratio in the right table', default=0.6, type=float)
    parser.add_argument('--threads',help='the number of threads', default=64, type=int)
    parser.add_argument('-K',help='the key size', default=8, type=int)
    parser.add_argument('--JD',help='the distribution of the right table [0: uniform, 1:normal, 2: beta, 3:zipf]', default=0, type=int)
    parser.add_argument('-k',help='the number of the most frequent-matching keys to be tracked', default=50000, type=int)
    parser.add_argument('--JD_NDEV',help='the standard deviation of the normal distribution if specified', default=1.0, type=float)
    parser.add_argument('--JD_ZALPHA',help='the alpha value of the zipfian distribution if specified', default=1.0, type=float)
    parser.add_argument('--TMP_OUT_DIR',help='the tmp output directory', default="/scratchNVM0/zczhu/tmp/", type=str)
    parser.add_argument('--OP',help='the path to output the result', default="emul-vary_buffer_size-cc-with-selectivity.txt", type=str)

    parser.add_argument('--IO',help='the IO latency in microseconds per I/O [def: 100 us]', default=100, type=float)

    args = parser.parse_args()
    main(args)

