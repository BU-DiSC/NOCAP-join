import os, math

R = 1000000
S = 8000000
F = 1.02
tries = 2
device = 'NVM1'

sync_io = False
ratio_list = [1,2,3,4,5]
JD = 3
buff_ratio_list = [0.25, 0.75]
last_buff_list = 'B_List = list(range(128, 512+1, 32))'
k_ratio = 0.05
origin_buff_list = last_buff_list
for ratio in ratio_list:
    new_R = R*ratio
    new_S = S*ratio
    k = round(new_R*k_ratio)
    buff_list = 'B_List = [' + str(int(round(buff_ratio_list[0]*math.sqrt(new_R*F/4))))
    for i in range(1, len(buff_ratio_list)):
        buff_list += ',' + str(int(round(buff_ratio_list[i]*math.sqrt(new_R*F/4))))
    buff_list += ']'
    #print(last_buff_list)
    os.system('sed -i "s/' + last_buff_list + '/' + buff_list + '/g" vary-buffer-size-emul.py')
    os.system('head -4 vary-buffer-size-emul.py | tail -1')
    last_buff_list = buff_list[:9] + "\\" + buff_list[9:-1] + "\\" + ']'
    suffix = '.txt'
    if not sync_io:
        suffix = '--no-sync-io.txt'
    cmd = 'python3 vary-buffer-size-emul.py --lTS ' + str(new_R) + ' --rTS ' + str(new_S) + ' --tries ' + str(tries)  + ' --JD ' + str(JD) + ' -k ' + str(k) + ' --OP=emul_vary_both_rels_ratio-zipf-' + str(ratio) + '-' + device + suffix
    #print(cmd)
    os.system(cmd)

os.system('sed -i "s/' + last_buff_list + '/' + origin_buff_list + '/g" vary-buffer-size-emul.py')
#os.system('head -4 vary-buffer-size-emul.py | tail -1')
