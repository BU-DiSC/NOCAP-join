# NOCAP: Near-Optimal Correlation-Aware Partitioning Joins

<H1> About </H1>

This repository is the codebase that of our work "NOCAP: Near-Optimal Correlation-Aware Partitioning Joins", which is accepted for publication in [SIGMOD 2024](https://dl.acm.org/doi/10.1145/3626739). We also have a longer version available [here](https://arxiv.org/abs/2310.03098).

Storage-based joins are still commonly used today because the memory budget does not always scale with the data size. One of the many join algorithms developed that has been widely deployed and proven to be efficient is the Hybrid Hash Join (HHJ), which is designed to exploit any available memory to maximize the data that is joined directly in memory. However, Hybrid Hash Join (HHJ) cannot fully exploit detailed knowledge of the join attribute correlation distribution. In this codebase, we emulate <b>Optimal Correlation-Aware Partitioning (OCAP)</b>, <b>Near-Optimal Correlation-Aware Partitioning (NOCAP)</b>, <b>Grace Hash Join (GHJ)</b>, <b>Sorted-Merge Join (SMJ)</b>, and <b>Dynamic Hybrid Hash Join (DHH)</b>, and we show that NOCAP can outperform the state of the art DHH for skewed correlations by up to 30%, and the textbook Grace Hash Join by up to 4X. 

<H1> Quick How-To </H1>

<H2> Compiling </H2> 

We use `CMAKE` to compile the code.

```
mkdir build
cd build && cmake ../ && make
```

<H1> Workload Generation </H1>

Run `./load-gen` to generate a workload. Two data files, `workload-rel-R.dat` and `workload-rel-S.dat` (default name) are generated, and each contains 1M, 8M records for relation R and S respectively. Another `workload-dis.txt` file is also produced which records the number of matching records of `S` for each primary key in `R`. You can also custimize the file name, the number of records by specifcying the parameters when running `load-gen`. More parameters can be found when running:

```
.\load-gen --help
```

<H2> Join Emulation </H2>

Assuming workload files are generated under build directory, you can then go into build directory and run 
```
.\emul --PJM-XXX -B [B] --NoSyncIO --mu 2.9 --tau 2.1 --NoJoinOutput
```

to run the join algorithm where `XXX` has to be specified as a join method (e.g., GHJ, SMJ, and DHH) and `B` specifies the number of pages as the available memory. More options can be found with `.\emul --help`. The read/write asymmetry may vary across different devices, you can run a set of experiments first to record the write latency and read latency (output by `emul`) and calculate the read/write asymmetry. `emul` supports two parameters $\mu$ (`--mu`) and $\tau$ (`--tau`) which specify the read/write asymmetry respectively for random write and sequential write. `NoSyncIO` means sync I/O is off ad `NoJoinOutput` means each join output page will be discarded once it is full. To run OCAP, specify the PJM as `HybridMatrixDP` and add `--RoundedHash` flag. For example:

```
.\emul --PJM-HybridMatrixDP -B 512 --RoundedHash --NoDirectIO --NoSyncIO --NoJoinOutput
```

In the above example, since OCAP is only used to do offline analysis, we usually turn direct I/O off to accelerate it ("using `--NoDirectIO`). NOCAP (Near-Optimal Correlation-Aware Partitioning Join) is an approximate version of OCAP, which runs as
```
.\emul --PJM-HybridApprMatrixDP -B 512 --RoundedHash --NoSyncIO --NoJoinOutput
```

<H2> Experiments & Notes </H2>

Before running any experiments, tune the `open files` in your system by `ulimit -n 65535` in case we are running out of file pointers. Note that the latency may differ from what we report in our paper due to different SSD devices, but \#I/Os should be similar when running the experiments. In addition, due to implementation issues, when allocating the memory in nested-loop join, we cannot allocate more than 4GB space and thus our prototype CANNOT SUPPORT a very large buffer size ( the input parameter `B` has to be smaller than 1024\*1024=1048576 so that the total memory budget is less than 4GB).

We use a 350GB Optane P4800X SSD with direct I/O enabled in our experiments, so if you have a different storage device, the exact latency in our experiments may differ. And if you are using a slower device, the experiments could take prohibitively long time. For example, with Amazon Nitro SSD, the main experiment would take at least a week. In addition, to produce the most matching patterns in our paper, you have to measure the read/write asymmetry mu, tau, nosync\_mu, and nosync\_tau, and pass them into the experimental script correctly. 

To do this, you need to compile the program and then run `./measure-read-write-asymmetry.sh` in the main directory to obtain the asymmetry numbers. You may see four numbers (representing mu, tau, nosync\_mu, nosync\_tau) output by the output. To run all the experiments with the emulated benchmark, go to `exp/` folder and run `./exp.sh [mu] [tau] [nosync_mu] [nosync_tau]` to generate the experiment results for Figures 8,9,10,11 and run `./part-stat-exp.sh` for Figure 4.

To run the TPC-H experiment, go to `tpch-exp/` folder and run `python3 tpch-q12-exp.py skewed`. 
To run the JCC-H experiment, go to `JCCH-exp/` folder and run `./jcch-setup.sh` and `python3 jcch-q12-exp.py`. 

To run the JOB experiment, go to `job-exp/` folder and run `./download-and-convert.sh` and then `python3 job-exp.py`

<H2> Plotting </H2>

After running all the experiments, we can then go to `plot-sources/` folder and plot all the figures by running `./main-scripts.sh [mu] [tau] [nosync_mu] [nosync_tau]`. All the figures are generated under the main folder.

Plotting requires `matplotlib` and `pandas` installed. In AWS ubuntu machine, you can use the following commands to install required packages:
```
sudo apt install python3-matplotlib python3-pandas
sudo apt install texlive texlive-latex-extra texlive-fonts-recommended dvipng cm-super
```
