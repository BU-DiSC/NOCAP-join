#ifndef PARAMETERS_H
#define PARAMETERS_H

#include<string>
#include "schema.h"

#define DB_PAGE_SIZE 4096
#define FUDGE_FACTOR 1.02

enum Dist {UNIFORM, NORMAL, ZIPFIAN, BETA};
enum PartitionedJoinMethod {GHJ, ApprMatrixDP, MatrixDP, NBJ, DynamicHybridHash, SMJ};
enum HashType {
    MD5 = 0x5U,
    SHA2 = 0x4U,
    MurMurhash = 0x3U,
    XXhash = 0x2U,
    CRC = 0x1U,
    CITY = 0x6U,
    RobinHood = 0x0U
};


typedef struct {
	long left_table_size = 1000000;
	long right_table_size = 8000000;
	uint32_t join_key_size = 8;
	ATTRIBUTE_TYPE join_key_type = STRING;
	// partition params
	uint32_t left_E_size = 1024;
	uint32_t right_E_size = 1024;
	uint32_t B = 320;
	uint32_t page_size = 4096;
	PartitionedJoinMethod pjm = MatrixDP;
	uint32_t NBJ_outer_rel_buffer = 1;
	bool SMJ_greater_flag = false;
	float DHH_skew_partition_percent = 0.02;
	float DHH_skew_frac_threshold = 0.01;
	uint32_t num_partitions = 319;
	HashType ht = MurMurhash;
	double randwrite_seqread_ratio = 3.5;
	double seqwrite_seqread_ratio = 1.2;
	uint32_t k = 5000;
	double hashtable_fulfilling_percent = 0.95;
	bool rounded_hash = false;
	bool no_smj_partition_wise_join = false;
	bool hybrid = false; // if MatrixDP/ApprMatrixDP supports hybrid hash join
	bool tpch_q12_flag = false;
	bool clct_part_stats_only_flag = false;
	bool tpch_q12_low_selectivity_flag = false;
	double left_selection_ratio = 1.0;
	double right_selection_ratio = 1.0;
	uint64_t left_selection_seed = 0;
	uint64_t right_selection_seed = 0;

	std::string workload_dis_path = "./workload-dis.txt";	
	std::string workload_rel_R_path = "./workload-rel-R.txt";	
	std::string workload_rel_S_path = "./workload-rel-S.txt";	
	std::string output_path = "./join-output.dat";
	std::string part_stats_path = "./part-stats.txt";
	std::string tpch_q12_path = "./Q12.sql";
        // distribution params
	Dist join_dist = UNIFORM;
	float join_dist_norm_stddev = 1.0;
	float join_dist_beta_alpha = 1.0;
	float join_dist_beta_beta = 1.0;
	float join_dist_zipf_alpha = 1.0;
	// The distribution of noise by default follows a normal distribution
	// with mean as 0. If the specified noise standard deviation is 0.0
	// percent, that means there is no noise. Otherwise, a noise generated
	// from a normal distribution N(0, x/100*(right_table_size/left_table_size
	// )) will be added where x is the specified percentage.
	float noise_stddev = 0.0;

	bool debug;
	bool no_direct_io;
	bool no_sync_io;
	bool no_join_output;

} Params;

#endif
