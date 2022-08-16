#!/bin/bash
BUFFER_RANGE="_126_379"
TRIES=3
DEVICE="NVM1"
echo "Baisc exp.."
#k_list=(20000 80000 140000 200000)
#for k in ${k_list[@]}
#do
#    python3 vary-buffer-size-emul.py --tries ${TRIES} -k ${k} --OP emul_vary_buffer_size${BUFFER_RANGE}-k-${k}-${DEVICE}.txt
#    python3 vary-buffer-size-emul.py --tries ${TRIES} -k ${k} --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-k-${k}-${DEVICE}.txt
#done

K_list=(4 8 16 32 64 128 256)
for K in ${K_list[@]}
do
    python3 vary-buffer-size-emul.py --tries ${TRIES} -K ${K} --OP emul_vary_buffer_size${BUFFER_RANGE}-${TRIES}-trials-key-size-${K}-${DEVICE}.txt
    python3 vary-buffer-size-emul.py --tries ${TRIES} -K ${K} --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-${TRIES}-trials-zipf-key-size-${K}-${DEVICE}.txt
done
#python3 vary-buffer-size-emul.py --tries ${TRIES} --OP emul_vary_buffer_size${BUFFER_RANGE}-new-2-GHJ-study.txt
#python3 vary-buffer-size-emul.py --tries ${TRIES} --OP emul_vary_buffer_size${BUFFER_RANGE}-SSD.txt
echo "Baisc exp done"

#<<COMMENT
echo "Varying rTS.."

<<COMMENT
rTS_scale_list=(4 8 16 32 64)
for rTS_scale in ${rTS_scale_list[@]}
do
    rTS=`echo "${rTS_scale}*1000000" | bc` 
    python3 vary-buffer-size-emul.py --tries ${TRIES} --rTS ${rTS} --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-${rTS_scale}M-${DEVICE}.txt
    python3 vary-buffer-size-emul.py --tries ${TRIES} --rTS ${rTS} --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-${rTS_scale}M-${DEVICE}.txt
done
COMMENT

#python3 vary-buffer-size-emul.py --rTS 5000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-5M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 7000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-7M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 9000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-9M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 11000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-11M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 13000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-13M-${DEVICE}.txt --tries ${TRIES}

#python3 vary-buffer-size-emul.py --rTS 5000000 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-5M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 7000000 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-7M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 9000000 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-9M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 11000000 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-11M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 13000000 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-rTS-13M-${DEVICE}.txt --tries ${TRIES}
#
#python3 vary-buffer-size-emul.py --rTS 20000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-20M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 24000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-24M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 28000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-28M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 32000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-32M.txt --tries ${TRIES}
#echo "Varying rTS done"

#echo "Varying both lTS and rTS.."

#python3 vary-buffer-size-emul.py --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-1M-rTS-8M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --lTS 2000000 --rTS 16000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-2M-rTS-16M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --lTS 3000000 --rTS 24000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-3M-rTS-24M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --lTS 4000000 --rTS 32000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-4M-rTS-32M-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --lTS 5000000 --rTS 40000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-5M-rTS-40M-${DEVICE}.txt --tries ${TRIES}

#python3 vary-buffer-size-emul.py --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-1M-rTS-8M-zipf-${DEVICE}.txt --tries ${TRIES} --JD 3
#python3 vary-buffer-size-emul.py --lTS 2000000 --rTS 16000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-2M-rTS-16M-zipf-${DEVICE}.txt --tries ${TRIES} --JD 3
#python3 vary-buffer-size-emul.py --lTS 3000000 --rTS 24000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-3M-rTS-24M-zipf-${DEVICE}.txt --tries ${TRIES} --JD 3
#python3 vary-buffer-size-emul.py --lTS 4000000 --rTS 32000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-4M-rTS-32M-zipf-${DEVICE}.txt --tries ${TRIES} --JD 3
#python3 vary-buffer-size-emul.py --lTS 5000000 --rTS 40000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-lTS-5M-rTS-40M-zipf-${DEVICE}.txt --tries ${TRIES} --JD 3


#
#python3 vary-buffer-size-emul.py --rTS 20000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-20M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 24000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-24M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 28000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-28M.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --rTS 32000000 --OP emul_vary_buffer_size${BUFFER_RANGE}-rTS-32M.txt --tries ${TRIES}
#echo "Varying both lTS and rTS done"

echo "Varying key size.."
#python3 vary-buffer-size-emul.py -K 4 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-4B-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 6 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-6B-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 8 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-8B-${DEVIC}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 10 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-10B-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 12 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-12B-${DEVICE}.txt --tries ${TRIES}

#python3 vary-buffer-size-emul.py -K 4 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-4B-zipf-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 6 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-6B-zipf-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 8 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-8B-zipf-${DEVIC}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 10 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-10B-zipf-${DEVICE}.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py -K 12 --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-12B-zipf-${DEVICE}.txt --tries ${TRIES}
echo "Varying key size done"


#COMMENT

<<COMMENT
#<<COMMENT

echo "Varying key size.."
python3 vary-buffer-size-emul.py -K 256 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-256B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py -K 512 --OP emul_vary_buffer_size${BUFFER_RANGE}-K-512B.txt --tries ${TRIES}
echo "Varying key size done"

#COMMENT

echo "Varying lE.."
python3 vary-buffer-size-emul.py --lE 128 --OP emul_vary_buffer_size${BUFFER_RANGE}-lE-128B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --lE 256 --OP emul_vary_buffer_size${BUFFER_RANGE}-lE-256B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --lE 512 --OP emul_vary_buffer_size${BUFFER_RANGE}-lE-512B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --lE 2048 --OP emul_vary_buffer_size${BUFFER_RANGE}-lE-2KB.txt --tries ${TRIES}
echo "Varying lE done"
echo "Varying rE.."
python3 vary-buffer-size-emul.py --rE 128 --OP emul_vary_buffer_size${BUFFER_RANGE}-rE-128B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --rE 256 --OP emul_vary_buffer_size${BUFFER_RANGE}-rE-256B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --rE 512 --OP emul_vary_buffer_size${BUFFER_RANGE}-rE-512B.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --rE 2048 --OP emul_vary_buffer_size${BUFFER_RANGE}-rE-2KB.txt --tries ${TRIES}
echo "Varying rE done"
echo "Varying distribution.."
python3 vary-buffer-size-emul.py --JD 1 --OP emul_vary_buffer_size${BUFFER_RANGE}-normal.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --JD 1 --JD_NDEV 0.25 --OP emul_vary_buffer_size${BUFFER_RANGE}-normal-dev-0.25.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --JD 1 --JD_NDEV 0.5 --OP emul_vary_buffer_size${BUFFER_RANGE}-normal-dev-0.5.txt --tries ${TRIES}
python3 vary-buffer-size-emul.py --JD 1 --JD_NDEV 2 --OP emul_vary_buffer_size${BUFFER_RANGE}-normal-dev-2.txt --tries ${TRIES}
COMMENT
#python3 vary-buffer-size-emul.py --JD 2 --OP emul_vary_buffer_size${BUFFER_RANGE}-beta.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 0.001 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-0.001.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 0.01 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-0.01.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 0.1 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-0.1.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-GHJ-study.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-SSD.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 1 --JD_NDEV 2 --OP emul_vary_buffer_size${BUFFER_RANGE}-normal-dev-2.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 0.5 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-0.5.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 0.8 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-0.8.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 1.2 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-1.2.txt --tries ${TRIES}
#python3 vary-buffer-size-emul.py --JD 3 --JD_ZALPHA 2 --OP emul_vary_buffer_size${BUFFER_RANGE}-zipf-alpha-2.txt --tries ${TRIES}
echo "Varying distribution done"

