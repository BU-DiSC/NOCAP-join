#!/bin/bash

TRIES=1

echo "Baisc exp.."
python3 vary-buffer-size.py --tries ${TRIES}
echo "Baisc exp done"

echo "Varying lTS.."
python3 vary-buffer-size.py --lTS 4000000 --OP vary_buffer_size-lTS-4M.txt --tries ${TRIES}
python3 vary-buffer-size.py --lTS 2000000 --OP vary_buffer_size-lTS-2M.txt --tries ${TRIES}
python3 vary-buffer-size.py --lTS 500000 --OP vary_buffer_size-lTS-0.5M.txt --tries ${TRIES}
echo "Varying lTS done"



#<<COMMENT
echo "Varying rTS.."
python3 vary-buffer-size.py --rTS 4000000 --OP vary_buffer_size-rTS-4M.txt --tries ${TRIES}
python3 vary-buffer-size.py --rTS 16000000 --OP vary_buffer_size-rTS-16M.txt --tries ${TRIES}
python3 vary-buffer-size.py --rTS 32000000 --OP vary_buffer_size-rTS-32M.txt --tries ${TRIES}
echo "Varying rTS done"
#COMMENT

#<<COMMENT

echo "Varying key size.."
python3 vary-buffer-size.py -K 256 --OP vary_buffer_size-K-256B.txt --tries ${TRIES}
python3 vary-buffer-size.py -K 512 --OP vary_buffer_size-K-512B.txt --tries ${TRIES}
echo "Varying key size done"

#COMMENT

echo "Varying lE.."
python3 vary-buffer-size.py --lE 128 --OP vary_buffer_size-lE-128B.txt --tries ${TRIES}
python3 vary-buffer-size.py --lE 256 --OP vary_buffer_size-lE-256B.txt --tries ${TRIES}
python3 vary-buffer-size.py --lE 512 --OP vary_buffer_size-lE-512B.txt --tries ${TRIES}
python3 vary-buffer-size.py --lE 2048 --OP vary_buffer_size-lE-2KB.txt --tries ${TRIES}
echo "Varying lE done"
echo "Varying rE.."
python3 vary-buffer-size.py --rE 128 --OP vary_buffer_size-rE-128B.txt --tries ${TRIES}
python3 vary-buffer-size.py --rE 256 --OP vary_buffer_size-rE-256B.txt --tries ${TRIES}
python3 vary-buffer-size.py --rE 512 --OP vary_buffer_size-rE-512B.txt --tries ${TRIES}
python3 vary-buffer-size.py --rE 2048 --OP vary_buffer_size-rE-2KB.txt --tries ${TRIES}
echo "Varying rE done"
echo "Varying distribution.."
python3 vary-buffer-size.py --JD 1 --OP vary_buffer_size-normal.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 1 --JD_NDEV 0.25 --OP vary_buffer_size-normal-dev-0.25.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 1 --JD_NDEV 0.5 --OP vary_buffer_size-normal-dev-0.5.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 1 --JD_NDEV 2 --OP vary_buffer_size-normal-dev-2.txt --tries ${TRIES}
#python3 vary-buffer-size.py --JD 2 --OP vary_buffer_size-beta.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 3 --JD_ZALPHA 0 --OP vary_buffer_size-zipf-alpha-0.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 3 --JD_ZALPHA 0.5 --OP vary_buffer_size-zipf-alpha-0.5.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 3 --JD_ZALPHA 1.5 --OP vary_buffer_size-zipf-alpha-1.5.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 3 --OP vary_buffer_size-zipf.txt --tries ${TRIES}
python3 vary-buffer-size.py --JD 3 --JD_ZALPHA 2 --OP vary_buffer_size-zipf-alpha-2.txt --tries ${TRIES}
echo "Varying distribution done"

