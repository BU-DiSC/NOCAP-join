# NOCAP: Near-Optimal Correlation-Aware Partitioning Joins

This repository contains a full version of our paper [full-version-NOCAP-with-appendix.pdf](./full-version-NOCAP-with-appendix.pdf) and also the codebase that emulates OCAP, NOCAP, Grace Hash Join (GHJ), Sorted-Merge Join (SMJ), and Dynamic Hybrid Hash Join (DHH) and it can be also used to run the experiments for our work "NOCAP: Near-Optimal Correlation-Aware Partitioning Joins".

<H1> Quick How-To </H1>

```
mkdir build
cd build && cmake ../ && make
```

<H1> Workload Generation </H1>
Run `./load-gen` to generate a workload. Two data files, workload-rel-R.dat and workload-rel-S.dat (default name) are generated, and each contains 1M, 8M records for relation R and S respectively. Another workload-dis.txt file is also produced which records to the number of matching records of S for each key from S. You can also custimize the file name, the number of records by specifcying the parameters when running `load-gen`. More parameters can be found when running:

```
.\load-gen --help
```

<H1> Join Emulation </H1>

Assuming workload files are generated under build directory, you can then go into build directory and run 
```
.\emul --PJM-XXX -B [B] --NoSyncIO --mu 2.9 --tau 2.1 --NoJoinOutput
```

to run the join algorithm where `XXX` has to be specified as a join method (e.g., GHJ, SMJ, and DHH) and `B` specifies the number of pages as the available memory. More options can be found with `emul --help`. The read/write asymmetry may vary across different devices, you can run a set of experiments first to record the write latency and read latency (output by `emul`) and calculate the read/write asymmetry. `emul` supports two parameters $\mu$ (`--mu`) and $\tau$ (`--tau`) which specify the read/write asymmetry respectively for random write and sequential write. `NoSyncIO` means sync I/O is off ad `NoJoinOutput` means each join output page will be discarded once it is full. To run OCAP, specify the PJM as `HybridMatrixDP` and add `--RoundedHash` flag. For example:

```
.\emul --PJM-HybridMatrixDP -B 512 --RoundedHash --NoDirectIO --NoSyncIO --NoJoinOutput
```

In the above example, since OCAP is only used to do offline analysis, we usually turn direct I/O off to accelerate it ("using `--NoDirectIO`). NOCAP (Near-Optimal Correlation-Aware Partitioning Join) is an approximate version of OCAP, which runs as
```
.\emul --PJM-HybridApprMatrixDP -B 512 --RoundedHash --NoSyncIO --NoJoinOutput
```

<H1> Experiments </H1>

Before running any experiments, tune the `open files` in your system by `ulimit -n 65535` in case we are running out of file pointers. Note that the latency may differ from what we report in our paper due to different SSD devices, but \#I/Os should be similar when running the experiments.

To run all the experiments with the emulated benchmark, go to `exp/` folder and run `./exp.sh` to generate the experiment results for Figures 1,7,8,9, and run `./part-stat-exp.sh` for Figure 4.

To run the TPC-H experiment, go to `tpch-exp/` folder and run `python3 tpch-q12-exp.py skewed`. 
