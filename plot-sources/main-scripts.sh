#!/bin/bash

#python3 CT-partition-size-plot.py
#python3 CT-partition-size-plot.py --dist=zipf --name="zipfian-distribution"
mu=${1:-"${nosync_mu}"}
tau=${2:-"${nosync_tau}"}
nosync_mu=${3:-"${mu}"}
nosync_tau=${4:-"${tau}"}


# Exp no-sync-io
python3 vary-large-buffer-size-emul-plot.py --suffix="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_0.7_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio-zipf-0.7"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.0_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="256-262144-mu-${nosync_mu}-tau-${nosync_mu}-nosyncio-zipf-1.0"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.3_256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio-zipf-1.3" --showLegend

python3 vary-large-buffer-size-emul-plot.py --io --suffix="256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${mu}-tau-${tau}"
python3 vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-zipf-0.7"
python3 vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_1.0_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-zipf-1.0"
python3 vary-large-buffer-size-emul-plot.py --io --suffix="zipf_alpha_1.3_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${nosync_mu}-tau-${nosync_tau}-zipf-1.3" --showLegend

# Exp sync-io
python3 vary-large-buffer-size-emul-plot.py --suffix="256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${mu}-tau-${tau}"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_0.7_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${mu}-tau-${tau}-zipf-0.7"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.0_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${mu}-tau-${tau}-zipf-1.0"
python3 vary-large-buffer-size-emul-plot.py --suffix="zipf_alpha_1.3_256-262144-mu-${mu}-tau-${tau}" --name="256-262144-mu-${mu}-tau-${tau}-zipf-1.3" --showLegend

# vary DHH thresholds
python3 vary-DHH-thresholds-emul.py --suffix="smaller_buff_emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --buff=512 --name="emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio-io" --io
python3 vary-DHH-thresholds-emul.py --suffix="smaller_buff_emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --buff=8192 --name="emul_vary_DHH_thresholds_zipf_alpha_0.7-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio-io" --io


# Tiny Memory
python3 vary-large-buffer-size-emul-plot.py --ymax=5 --suffix="128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --showLegend --plainXaxis --legendLocation='lower left'
python3 vary-large-buffer-size-emul-plot.py --ymax=5 --suffix="zipf_alpha_1.0_128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --name="zipf_alpha_1.0_128-512-mu-${nosync_mu}-tau-${nosync_tau}-nosyncio" --plainXaxis --legendLocation='lower left'



# TPC-H
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SEL-0.48-SF10-nosyncio" --name="tpch-q12-SF10-SEL-0.48-nosyncio" --ymax=10 --showLegend --legendLocation='lower left'
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SEL-0.63-SF10-nosyncio" --name="tpch-q12-SF10-SEL-0.63-nosyncio" --ymax=12
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SEL-0.48-SF50-nosyncio" --name="tpch-q12-SF50-SEL-0.48-nosyncio" --ymax=60 --showLegend --legendLocation='lower left'
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="tpch-q12-SEL-0.63-SF50-nosyncio" --name="tpch-q12-SF50-SEL-0.63-nosyncio" --ymax=70

#JCCH
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SF10-SEL-0.48-nosyncio" --name="jcch-q12-SF10-SEL-0.48-nosyncio" --ymax=30

python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SF10-SEL-0.63-nosyncio" --name="jcch-q12-SF10-SEL-0.63-nosyncio" --ymax=35
#python3 .\dataset-DHH-vs-NOCAP-emul-plot.py --suffix="jcch-q12-SEL-0.63-scaling-4-nosyncio" --name="jcch-q12-SF4-SEL-0.63-nosyncio" --ymax=25

#JOB
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="job-exp-emul-cast-title-skewed-nosyncio" --name="job-exp-emul-cast-title-skewed-nosyncio" --ymax 30
python3 dataset-DHH-vs-NOCAP-emul-plot.py --suffix="job-exp-emul-cast-name-skewed-nosyncio" --name="job-exp-emul-cast-name-skewed-nosyncio" --ymax=25
COMMENT
