#!/bin/bash

#Intro Exp
#python3 vary-buffer-size-emul-fig1.py --OP="emul_vary_buffer_size_128-250000-mu-1-tau-1-nodirectio.txt"  --tries 3
#python3 vary-buffer-size-emul-fig1.py --OP="emul_vary_buffer_size_zipf_128-250000-mu-1-tau-1-nodirectio.txt" --JD 3 --tries 3

#Exp 1
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_256-262144-mu-1.5-tau-1.43-nosyncio.txt"  --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-1.5-tau-1.43-nosyncio.txt" --JD 3 --JD_ZALPHA 1.3 --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-1.5-tau-1.43-nosyncio.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-1.5-tau-1.43-nosyncio.txt" --JD 3 --JD_ZALPHA 0.7 --tries 3

#Exp 2
python3 vary-buffer-size-emul-fig7-direct-io.py --OP="emul_vary_buffer_size_256-262144-mu-5-tau-3.5.txt"  --tries 3
python3 vary-buffer-size-emul-fig7-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 1.3 --tries 3
python3 vary-buffer-size-emul-fig7-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3
python3 vary-buffer-size-emul-fig7-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-5-tau-3.5.txt" --JD 3 --JD_ZALPHA 0.7 --tries 3



#Exp 3
python3 vary-buffer-size-emul-fig8.py --OP="emul_vary_buffer_size_128-512-mu-2.9-tau-2.1-nosyncio.txt"  --tries 2
python3 vary-buffer-size-emul-fig8.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_128-512-mu-2.9-tau-2.1-nosyncio.txt"  --tries 2 --JD 3 --JD_ZALPHA 1.0

#microbenchmark
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-5K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 5000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-10K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 10000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-20K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 20000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-30K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 30000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-40K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 40000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-60K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 60000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-80K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 80000
