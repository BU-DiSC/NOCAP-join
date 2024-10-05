import os, math, time, copy, sys
PJM_List = ['DHH --DHH_skew_frac_threshold=0.0', 'DHH', 'HybridApprMatrixDP --RoundedHash']
shared_params = " --NoJoinOutput --tpch-q12 --rSR 0.63 --NoSyncIO --mu 1.2 --tau 1.14"
shared_params2 = " --NoJoinOutput --tpch-q12 --rSR 0.488 --NoSyncIO --mu 1.2 --tau 1.14"
#shared_params = " --NoJoinOutput --tpch-q12 --rSR 0.11 --NoSyncIO --mu 1.2 --tau 1.1"
scale_ratio_list = [10, 50]

buff_list = [int(10*2**(x//2+8)) if x//2 == 0 else int(5*(2**(x//2 + 8) + 2**(x//2 + 9))) for x in range(13)]
F = 1.02
tries = 3


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
result2 = [[{} for pjm in PJM_List] for i in range(len(buff_list))]
size_of_orders_record = 184
k_ratio = 0.05
num_entries_per_page = math.floor(4096/184)
origin_scale_ratio = 1
origin_scale_str = 's ' + str(origin_scale_ratio)
os.system('rm part_rel_R/* && rm part_rel_S/*')
os.system('cp dbgen/qgen ./ && cp dbgen/queries/12.sql ./')
for scale_ratio in scale_ratio_list:
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
    os.system('../build/data-converter --CSV2DAT --right-table-input-path=data/lineitem.csv --right-table-output-path=workload-rel-S.dat --left-table-input-path=data/orders.csv --left-table-output-path=workload-rel-R.dat')
    os.system('rm data/lineitem.csv && rm data/orders.csv')
    os.system('./qgen -a 12 -b dbgen/dists.dss > Q12.sql')
    os.system('sed -i "s/.*and l_shipmode in.*/\tand l_shipmode in (\'AIR\', \'FOB\', \'MAIL\', \'RAIL\', \'REG AIR\', \'SHIP\', \'TRUCK\')/g" Q12.sql')
    f = open('workload-dis.txt','r')
    R = len(f.readlines()) - 1
    print('#records in orders: ' + str(R))
    f.close()
    os.system("sync")
    time.sleep(2)
    for i, buff in enumerate(buff_list):
        for j, pjm in enumerate(PJM_List):
            for t in range(tries):
                #print(buff_ratio*math.sqrt(scale_ratio*math.ceil(R*1.0/num_entries_per_page)*F))
                cmd = '../build/emul -B ' + str(buff) + ' --PJM-' + pjm + shared_params + ' -k ' + str(5000*scale_ratio)
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
    if sys.argv[1] != 'skewed':
        output(result, 'tpch-q12-SEL-0.63-SF' + str(scale_ratio) + suffix) 
        #output(result, 'tpch-q12-all-year-all-shipmode-exp-emul-scaling-' + str(scale_ratio) + suffix) 
    else:
        output(result, 'tpch-q12-SEL-0.63-skewed-SF' + str(scale_ratio) + suffix) 
        #output(result, 'tpch-q12-all-year-all-shipmode-exp-emul-skewed-scaling-' + str(scale_ratio) + suffix) 
    time.sleep(2)
    for i, buff in enumerate(buff_list):
        for j, pjm in enumerate(PJM_List):
            for t in range(tries):
                #print(buff_ratio*math.sqrt(scale_ratio*math.ceil(R*1.0/num_entries_per_page)*F))
                cmd = '../build/emul -B ' + str(buff) + ' --tpch-q12-low-selectivity --PJM-' + pjm + shared_params2 + ' -k ' + str(5000*scale_ratio)
                print(cmd)
                os.system(cmd + ' > output.txt')
                tmp = parse_output('output.txt')
                print("Finish " + pjm + " with cost time: " + str(tmp['total']))
                print("I/O cnt: " + str(tmp['read_pages_tt'] + tmp['write_pages_tt']))
                print("Output #entries: " + str(tmp['output_entries']))
                os.system('rm output.txt')
                result2[i][j] = merge(result2[i][j], tmp)
    for i in range(len(buff_list)):
        for j in range(len(PJM_List)):
            for k in result2[i][j]:
                result2[i][j][k] /= tries*1.0
    suffix = "-nosyncio.txt"
    if sys.argv[1] != 'skewed':
        output(result2, 'tpch-q12-SEL-0.488-SF' + str(scale_ratio) + suffix) 
    else:
        output(result2, 'tpch-q12-SEL-0.488-skewed-SF' + str(scale_ratio) + suffix) 
os.system('rm qgen')
