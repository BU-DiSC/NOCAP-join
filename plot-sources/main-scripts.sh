#!/bin/bash

# CT plots
alias python='python3'
#python CT-partition-size-plot.py
#python CT-partition-size-plot.py --dist=zipf --name="zipfian-distribution"

# Exp no-sync-io
python vary-large-buffer-size-emul-plot.py --suffix="256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio"
python vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_0.7_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio-zipf-0.7"
python vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.0_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.28-nosyncio-zipf-1.0"
python vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.3_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio-zipf-1.3" --showLegend
python vary-large-buffer-size-emul-plot.py --io --suffix="256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio"
python vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_0.7_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio-zipf-0.7"
python vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_1.0_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio-zipf-1.0"
python vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_1.3_256-262144-mu-1.28-tau-1.2-nosyncio" --name="256-262144-mu-1.28-tau-1.2-nosyncio-zipf-1.3" --showLegend

# Exp sync-io
python vary-large-buffer-size-emul-plot.py --ymax=7 --suffix="256-262144-mu-3.3-tau-3.2" --name="256-262144-mu-3.3-tau-3.2"
python vary-large-buffer-size-emul-plot.py --ymax=7 --suffix="zipf_alpha_0.7_256-262144-mu-3.3-tau-3.2" --name="256-262144-mu-3.3-tau-3.2-zipf-0.7"
python vary-large-buffer-size-emul-plot.py --ymax=7 --suffix="zipf_alpha_1.0_256-262144-mu-3.3-tau-3.2" --name="256-262144-mu-3.3-tau-3.2-zipf-1.0"
python vary-large-buffer-size-emul-plot.py --ymax=7 --suffix="zipf_alpha_1.3_256-262144-mu-3.3-tau-3.2" --name="256-262144-mu-3.3-tau-3.2-zipf-1.3" --showLegend

# vary DHH thresholds
python vary-DHH-thresholds-emul.py --suffix="smaller_buff_emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-1.28-tau-1.2-nosyncio" --buff=512 --name="emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-1.28-tau-1.2-nosyncio-io" --io
python vary-DHH-thresholds-emul.py --suffix="smaller_buff_emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-1.28-tau-1.2-nosyncio" --buff=8192 --name="emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-1.28-tau-1.2-nosyncio-io" --io


# Tiny Memory
python vary-large-buffer-size-emul-plot.py --ymax=5 --suffix="128-512-mu-1.28-tau-1.2-nosyncio" --name="128-512-mu-1.28-tau-1.2-nosyncio" --showLegend --plainXaxis --legendLocation='lower left'
python vary-large-buffer-size-emul-plot.py --ymax=5 --suffix="zipf_alpha_1.0_128-512-mu-1.28-tau-1.2-nosyncio" --name="zipf_alpha_1.0_128-512-mu-1.28-tau-1.2-nosyncio" --plainXaxis --legendLocation='lower left'



# TPC-H
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SF10-SEL-0.48-nosyncio" --name="tpch-q12-SF10-SEL-0.48-nosyncio" --ymax=10 --showLegend --legendLocation='lower left'
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SF10-SEL-0.63-nosyncio" --name="tpch-q12-SF10-SEL-0.63-nosyncio" --ymax=12
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SF50-SEL-0.48-nosyncio" --name="tpch-q12-SF50-SEL-0.48-nosyncio" --ymax=60 --showLegend --legendLocation='lower left'
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SF50-SEL-0.63-nosyncio" --name="tpch-q12-SF50-SEL-0.63-nosyncio" --ymax=70

#JCCH
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SF10-SEL-0.48-nosyncio" --name="jcch-q12-SF10-SEL-0.48-nosyncio" --ymax=30
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SF10-SEL-0.63-nosyncio" --name="jcch-q12-SF10-SEL-0.63-nosyncio" --ymax=35
#python .\dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SEL-0.63-scaling-4-nosyncio" --name="jcch-q12-SF4-SEL-0.63-nosyncio" --ymax=25

#JOB
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="job-exp-emul-cast-title-skewed-nosyncio" --name="job-exp-emul-cast-title-skewed-nosyncio" --ymax 30
python dataset-DHH-vs-NOCAP-emul-plot.py --suffix="job-exp-emul-cast-name-skewed-nosyncio" --name="job-exp-emul-cast-name-skewed-nosyncio" --ymax=25
