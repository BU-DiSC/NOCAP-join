import os, math, time, copy, sys
PJM_List = ['DHH --DHH_skew_frac_threshold=0.0', 'DHH', 'HybridApprMatrixDP --RoundedHash']
shared_params = " --NoJoinOutput --NoSyncIO --mu 1.2 --tau 1.14"

buff_list = [int(10*2**(x+9)) for x in range(7)]
F = 1.02
tries = 2

metric_mapping = {
        'Join Time':['total',-2], 
        'Output #entries':['output_entries',-1], 
        'Total Read #pages:':['read_pages_tt',-1],
        'Total Write #pages':['write_pages_tt',-1],
        'I/O Time':['io',-2],
        'Partition Time':['partition',-2],
        'Sequential Write #pages':['seq_write_pages_tt',-1],
        'Probe Time':['probe',-2],
        'Normalized I/Os':['normalized_io_tt',-1],
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
    for i, buff in enumerate(buff_list):
        f.write(str(buff))
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


print(buff_list)
result = [[{} for pjm in PJM_List] for i in range(len(buff_list))]


for t in range(tries):
    os.system("sync")
    time.sleep(2)
    for i, buff in enumerate(buff_list):
        for j, pjm in enumerate(PJM_List):
            cmd = '../build/emul -B ' + str(buff) + ' --PJM-' + pjm + shared_params + ' -k ' + str(50000)
            print(cmd)
            os.system(cmd + ' > output.txt')
            tmp = parse_output('output.txt')
            print("Finish " + pjm + " with cost time: " + str(tmp['total']))
            print("I/O cnt: " + str(tmp['read_pages_tt'] + tmp['write_pages_tt']))
            print("Output #entries: " + str(tmp['output_entries']))
            os.system('rm output.txt')
            result[i][j] = merge(result[i][j], tmp)
for i in range(len(buff_list)):
    for j in range(len(PJM_List)):
        for k in result[i][j]:
            result[i][j][k] /= tries*1.0

suffix = "-nosyncio.txt"

output(result, 'job-exp-emul-skewed-' + suffix) 

