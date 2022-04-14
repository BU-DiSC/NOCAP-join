#ifndef PARAMETERS_H
#define PARAMETERS_H

#include<string>

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


struct Params{
	long left_table_size;
	long right_table_size;
	uint32_t K;
	// partition params
	uint32_t left_E_size;
	uint32_t right_E_size;
	uint32_t B;
	uint32_t page_size;
	PartitionedJoinMethod pjm;
	uint32_t NBJ_outer_rel_buffer;
	bool SMJ_greater_flag;
	uint32_t num_partitions;
	HashType ht;
	double randwrite_seqread_ratio;
	double seqwrite_seqread_ratio;
	uint32_t k;
	uint32_t k_max;
	double hashtable_fulfilling_percent;
	bool rounded_hash;
	double c;
	double th; 
	bool hybrid;
	bool tpch_flag;
	bool clct_part_meta_only_flag;

	std::string workload_dis_path;	
	std::string workload_rel_R_path;	
	std::string workload_rel_S_path;	
	std::string output_path;
        // distribution params
	Dist join_dist;
	float join_dist_norm_stddev;
	float join_dist_beta_alpha;
	float join_dist_beta_beta;
	float join_dist_zipf_alpha;

	bool debug;
	bool no_direct_io;
	bool no_join_output;
};

#endif
