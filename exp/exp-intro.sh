#!/bin/bash

#Intro Exp
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_128-250000-mu-1-tau-1-nodirectio.txt"  --tries 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_128-250000-mu-1-tau-1-nodirectio.txt" --JD 3 --tries 3

#Exp 1
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_256-262144-mu-2.9-tau-2.1-nosyncio.txt"  --tries 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-2.9-tau-2.1-nosyncio.txt" --JD 3 --JD_ZALPHA 1.3 --tries 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-2.9-tau-2.1-nosyncio.txt" --JD 3 --JD_ZALPHA 0.7 --tries 3

#Exp 2
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_256-262144-mu-5-tau-3.5.txt"  --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 1.3 --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 0.7 --tries 3



#Exp 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_128-512-mu-2.9-tau-2.1-nosyncio.txt"  --tries 3
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_128-512-mu-2.9-tau-2.1-nosyncio.txt"  --tries 3 --JD 3 --JD_ZALPHA 1.0
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_512-250000-mu-2.6-tau-2.1-nosyncio.txt"
#python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_512-250000-mu-2.6-tau-2.1-nosynctio.txt" --JD 3
