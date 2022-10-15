import os, math, time, copy, sys
sync_io=True
PJM_List = ['GHJ', 'ApprMatrixDP --RoundedHash', 'SMJ']
shared_params = " --NoJoinOutput --tpch"
scale_ratio_list = [1]

buff_ratio_list = [0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0]
F = 1.02
tries = 3


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



def output(result, filename, num_pages_in_R):
    f = open(filename,'w')
    f.write('buffer')
    for pjm in PJM_List:
        s = ""
        for key in metric_mapping:
            s += ',' + metric_mapping[key][0] + '-' + pjm
            
        f.write(s)
    f.write('\n')
    for i, buff_ratio in enumerate(buff_ratio_list):
        f.write(str(int(round(buff_ratio*math.sqrt(num_pages_in_R*F)))))
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



if sync_io:
    for i in range(len(PJM_List)):
        if PJM_List[i] != 'SMJ':
            PJM_List[i] = PJM_List[i] + ' --mu 2.4 --tau 2.2'
    shared_params += ' --NoSyncIO'


result = [[{} for pjm in PJM_List] for i in range(len(buff_ratio_list))]
size_of_orders_record = 184
k_ratio = 0.05
num_entries_per_page = math.floor(4096/184)
origin_scale_ratio = 1
origin_scale_str = 's ' + str(origin_scale_ratio)
for scale_ratio in scale_ratio_list:
    for t in range(tries):
        if sys.argv[1] != 'skewed':
            os.system('sed -i "s/' + origin_scale_str + '/s ' + str(scale_ratio) + '/g" ./tpch-setup.sh')
            print('setup uniform workload')
            os.system('./tpch-setup.sh')
            os.system('sed -i "s/s ' + str(scale_ratio)  + '/' + origin_scale_str + '/g" ./tpch-setup.sh')
        else:
            os.system('sed -i "s/' + origin_scale_str + '/s ' + str(scale_ratio) + '/g" ./tpch-skewed-setup.sh')
            print('setup skewed workload')
            os.system('./tpch-skewed-setup.sh')
            os.system('sed -i "s/s ' + str(scale_ratio)  + '/' + origin_scale_str + '/g" ./tpch-skewed-setup.sh')
        os.system('../build/tpch-converter --CSV2DAT --lineitem-input=data/lineitem.csv --lineitem-output=workload-rel-S.dat --orders-input=data/orders.csv --orders-output=workload-rel-R.dat')
        f = open('workload-dis.txt','r')
        R = len(f.readlines()) - 1
        print('#records in orders: ' + str(R))
        f.close()

        os.system("sync")
        time.sleep(2)
        for i, buff_ratio in enumerate(buff_ratio_list):
            for j, pjm in enumerate(PJM_List):
                #print(buff_ratio*math.sqrt(scale_ratio*math.ceil(R*1.0/num_entries_per_page)*F))
                cmd = '../build/emul -B ' + str(int(round(buff_ratio*math.sqrt(scale_ratio*math.ceil(R*1.0/num_entries_per_page)*F)))) + ' --PJM-' + pjm + shared_params + ' -k ' + str(round(scale_ratio*R*k_ratio))
                print(cmd)
                os.system(cmd + ' > output.txt')
                tmp = parse_output('output.txt')
                print("Finish " + pjm + " with cost time: " + str(tmp['total']))
                print("I/O cnt: " + str(tmp['read_pages_tt'] + tmp['write_pages_tt']))
                print("Output #entries: " + str(tmp['output_entries']))
                os.system('rm output.txt')
                result[i][j] = merge(result[i][j], tmp)
        os.system('rm workload-rel-R.dat')
        os.system('rm workload-rel-S.dat')
    for i in range(len(buff_ratio_list)):
        for j in range(len(PJM_List)):
            for k in result[i][j]:
                result[i][j][k] /= tries*1.0
    suffix = "-beta-0.1.txt"
    if sync_io:
        suffix = "-no-sync-io-beta-0.1.txt"
    if sys.argv[1] != 'skewed':
        output(result, 'tpch-exp-emul-scaling-' + str(scale_ratio) + suffix, math.ceil(scale_ratio*R*1/num_entries_per_page)) 
    else:
        output(result, 'tpch-exp-emul-skewed-scaling-' + str(scale_ratio) + suffix, math.ceil(scale_ratio*R*1/num_entries_per_page)) 


