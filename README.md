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

<H1> Experiments & Notes </H1>

Before running any experiments, tune the `open files` in your system by `ulimit -n 65535` in case we are running out of file pointers. Note that the latency may differ from what we report in our paper due to different SSD devices, but \#I/Os should be similar when running the experiments. In addition, due to implementation issues, when allocating the memory in nested-loop join, we cannot allocate more than 4GB space and thus our prototype CANNOT SUPPORT a very large buffer size ( the input parameter `B` has to be smaller than 1024\*1024=1048576 so that the total memory budget is less than 4GB).

We use a 350GB PCIe P4510 SSD with direct I/O enabled in our experiments, so if you have a different storage device, the exact latency in our experiments may differ. To produce the most matching patterns in our paper, you have to measure the read/write asymmetry mu, tau, nosync\_mu, and nosync\_tau, and pass them into the experimental script correctly.

To do this, you need to compile the program and then run `./measure-read-write-asymmetry.sh` in the main directory to obtain the asymmetry numbers. You may see four numbers (representing mu, tau, nosync\_mu, nosync\_tau) output by the output. To run all the experiments with the emulated benchmark, go to `exp/` folder and run `./exp.sh [mu] [tau] [nosync_mu] [nosync_tau]` to generate the experiment results for Figures 8,9,10,11 and run `./part-stat-exp.sh` for Figure 4.

To run the TPC-H experiment, go to `tpch-exp/` folder and run `python3 tpch-q12-exp.py skewed`. 
To run the JCC-H experiment, go to `JCCH-exp/` folder and run `./jcch-setup.sh` and `python3 jcch-q12-exp.py`. 

To run the JOB experiment, go to `job-exp/` folder and run `./download-and-convert.sh` and then `python3 job-exp.py`

<H1> Plotting </H1>

After running all the experiments, we can then go to `plot-sources/` folder and plot all the figures by running `./main-scripts.sh [mu] [tau] [nosync_mu] [nosync_tau]`. All the figures are generated under the main folder.

Plotting requires `matplotlib` and `pandas` installed. In AWS ubuntu machine, you can use the following commands to install required packages:
```
sudo apt install python3-matplotlib python3-pandas
sudo apt install texlive texlive-latex-extra texlive-fonts-recommended dvipng cm-super
```
