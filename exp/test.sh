#!/bin/bash
python3 vary-buffer-size-emul.py --lTS=4000 --rTS=32000 --OP vary_buffer_size-288-352-BNLJ-first-4000_rTS-32000.txt --tries 10
python3 vary-buffer-size-emul.py --lTS=5000 --rTS=40000 --OP vary_buffer_size-288-352-BNLJ-first-5000_rTS-40000.txt --tries 10
python3 vary-buffer-size-emul.py --lTS=6000 --rTS=48000 --OP vary_buffer_size-288-352-BNLJ-first-6000_rTS-48000.txt --tries 10
