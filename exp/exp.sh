#!/bin/bash
ulimit -n 65535
#Intro Exp
#python3 vary-buffer-size-emul-fig1.py --OP="emul_vary_buffer_size_128-250000-mu-1-tau-1-nodirectio.txt"  --tries 3
#python3 vary-buffer-size-emul-fig1.py --OP="emul_vary_buffer_size_zipf_128-250000-mu-1-tau-1-nodirectio.txt" --JD 3 --tries 3
mu=${1:-"1.28"}
tau=${2:-"1.2"}
nosync_mu=${3:-"3.3"}
nosync_tau=${4:-"3.2"}
tries=1
#Exp 1
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_256-262144-mu-${mu}-tau-${tau}.txt"  --tries ${tries} --mu ${mu} --tau ${tau}
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-${mu}-tau-${tau}.txt" --JD 3 --JD_ZALPHA 1.3 --tries ${tries} --mu ${mu} --tau ${tau}
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-${mu}-tau-${tau}.txt" --JD 3 --JD_ZALPHA 1.0 --tries ${tries} --mu ${mu} --tau ${tau}
python3 vary-buffer-size-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}.txt" --JD 3 --JD_ZALPHA 0.7 --tries ${tries} --mu ${mu} --tau ${tau} 

#Exp 2
python3 vary-buffer-size-emul-fig8-direct-io.py --OP="emul_vary_buffer_size_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio.txt"  --tries ${tries} --mu ${nosync_mu} --tau ${nosync_tau}
python3 vary-buffer-size-emul-fig8-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_1.3_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio.txt" --JD 3 --JD_ZALPHA 1.3 --tries ${tries}  --mu ${nosync_mu} --tau ${nosync_tau}
python3 vary-buffer-size-emul-fig8-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio.txt" --JD 3 --JD_ZALPHA 1.0 --tries ${tries}  --mu ${nosync_mu} --tau ${nosync_tau}
python3 vary-buffer-size-emul-fig8-direct-io.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio.txt" --JD 3 --JD_ZALPHA 0.7 --tries ${tries}  --mu ${nosync_mu} --tau ${nosync_tau}




#Exp 3
python3 vary-buffer-size-emul-fig9.py --OP="emul_vary_buffer_size_128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio.txt"  --tries ${tries} --mu ${nosync_mu} --tau ${nosync_tau} 
python3 vary-buffer-size-emul-fig9.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio-rerun.txt"  --tries ${tries} --JD 3 --JD_ZALPHA 1.0 --mu ${nosync_mu} --tau ${nosync_tau} 

python3 vary-buffer-size-emul-DHH.py --OP="smaller_buff_emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-${mu}-tau-${tau}-nosyncio.txt" --tries ${tries} --JD 3 --JD_ZALPHA 0.7 --mu ${nosync_mu} --tau ${nosync_tau}
python3 vary-buffer-size-emul-DHH.py --OP="smaller_buff_emul_vary_DHH_thresholds-mu-${mu}-tau-${tau}-nosyncio.txt" --tries ${tries} --mu ${nosync_mu} --tau ${nosync_tau}
#microbenchmark
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-5K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 5000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-10K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 10000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-20K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 20000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-30K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 30000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-40K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 40000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-60K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 60000
#python3 vary-buffer-size-emul-micro.py --OP="emul_vary_buffer_size_zipf_alpha_1.0_256-262144-mu-2.9-tau-2.1-nosyncio-k-80K.txt" --JD 3 --JD_ZALPHA 1.0 --tries 3 -k 80000


python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}-noise-stddev-8-nosyncio.txt" --tries ${tries} --JD 3 --JD_ZALPHA 0.7 --NOISE_STDDEV 8 --mu ${nosync_mu} --tau ${nosync_tau}
python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_256-262144-mu-${mu}-tau-${tau}-noise-stddev-8-nosyncio.txt" --tries ${tries} --NOISE_STDDEV 8 --mu ${nosync_mu} --tau ${nosync_tau}

#python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}-noise-stddev-15-nosyncio.txt" --tries ${tries} --JD 3 --JD_ZALPHA 0.7 --NOISE_STDDEV 15
#python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}-noise-stddev-100-nosyncio.txt" --tries ${tries} --JD 3 --JD_ZALPHA 0.7 --NOISE_STDDEV 100
#python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_256-262144-mu-${mu}-tau-${tau}-noise-stddev-15-nosyncio.txt" --tries ${tries} --NOISE_STDDEV 15
#python3 vary-buffer-size-noise-emul.py --OP="emul_vary_buffer_size_zipf_alpha_0.7-256-262144-mu-${mu}-tau-${tau}-noise-stddev-10.txt" --tries ${tries} --JD 3 --JD_ZALPHA 0.7 --NOISE_STDDEV 10
