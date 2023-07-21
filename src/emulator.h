#ifndef EMULATOR_H
#define EMULATOR_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <functional>
#include <random>
#include <tuple>

#include "parameters.h"
#include "schema.h"

#define TPCH_Q12_YEAR_ROUGHLY_MATCH true
#define TPCH_Q12_YEAR_OFFSET 4

struct Cut{
    uint64_t cost;
	uint32_t lastPos;
	Cut(){
	    cost = UINT64_MAX;
	    lastPos = 0U;
	}
};

class Emulator {
	Params params_;
public:
	uint32_t left_entries_per_page;
	uint32_t right_entries_per_page;
	uint64_t left_selection_seed;
	uint64_t right_selection_seed;
	uint32_t step_size = 0;
	double join_duration = 0;
	double probe_duration = 0;
	double partition_duration = 0;
	double algo_duration = 0;
	double partition_cpu_duration = 0;
	double partition_io_duration = 0;
	double io_duration = 0;
	double read_duration = 0;
	double output_duration = 0;
	uint32_t output_cnt = 0;
	uint32_t read_cnt = 0;
	uint32_t write_cnt = 0;
	uint32_t seq_write_cnt = 0;
	uint32_t output_write_cnt = 0;

	int join_output_fd = -1;
	char* join_output_buffer;
	uint32_t join_output_offset = 0;
	uint32_t join_entry_size;
    uint32_t join_output_entries_counter = 0;
    uint32_t join_output_entries_per_page = 0;
	bool opened = false;
	bool rounded_hash = false;

        char* R_rel_buffer = nullptr;
        char* S_rel_buffer = nullptr;

    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity; 
	std::vector<std::tuple<std::string, uint32_t, uint32_t> >* tpch_q12_results = nullptr;
	int tpch_q12_required_year;
    std::uniform_real_distribution<double> selection_dist; 

	Emulator(Params & params);

	void write_and_clear_one_page(int fd, char* src);
	void add_one_record_into_join_result_buffer(const char* src1, size_t len1, const char* src2, size_t len2);
	ssize_t read_one_page(int fd, char* src);
	void finish();

    void clct_partition_stats();	

	void get_emulated_cost();

	// Nested Block Join
	void get_emulated_cost_NBJ();
	void get_emulated_cost_NBJ(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth, uint32_t left_filter_condition = 0, uint32_t right_filter_condition = 0, bool hash = false);

	// Sort Merge Join
	template <typename T> void internal_sort(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_entries, double selection_ratio, uint64_t* selection_seed, uint32_t filter_condition, std::vector<uint32_t> & num_entries_per_run);
	template <typename T> void merge_sort_for_one_pass(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no, std::vector<uint32_t> & num_entries_per_run);
	template <typename T> void merge_join(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint8_t left_pass_no, uint8_t right_pass_no, std::vector<uint32_t> left_num_entries_per_run, std::vector<uint32_t> right_num_entries_per_run);
	void get_emulated_cost_SMJ();
	void get_emulated_cost_SMJ(std::string left_file_name, std::string right_file_name, std::string left_prefix, std::string right_prefix, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	// GHJ
	void get_emulated_cost_GHJ();
	void get_emulated_cost_GHJ(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);

	// Dynamical Hybrid Hash
	void get_emulated_cost_DHH();
	void get_emulated_cost_DHH(std::string left_file_name, std::string right_file_name, uint32_t depth);

	// MatrixDP
	uint64_t get_probe_cost(uint32_t & m_r,  uint32_t entries_from_R, uint32_t offset, uint32_t step_size,  std::vector<uint32_t> & SumSoFar, const Params & params, Cut** cut_matrix);
	uint64_t est_probe_cost(uint32_t & num_of_random_in_memory_partitions, uint32_t & num_of_random_in_memory_entries, uint32_t m_r, uint32_t entries_from_S, uint32_t entries_from_R, uint32_t step_size, bool one_page_used_for_hybrid_join, const Params & params);
	// Two versions of populating the cut matrix (one is designed for the approximate algorithm and the other is designed for the optimal algorithm)
	uint64_t est_GHJ_cost(double entries_from_R, double entries_from_S);
	// Although most logic between the two versions are similar, since the sorted order of key_multiplicity is different, there are many minor differences between the two versions
	uint64_t cal_cost(uint32_t start_idx, uint32_t end_idx, std::vector<uint32_t> & SumSoFar);
	void populate_cut_matrix(uint32_t n, uint32_t m, uint32_t offset, bool appr_flag, std::vector<uint32_t> & SumSoFar, Params & params, uint64_t & min_cost, uint32_t & num_partitions, Cut** cut_matrix);
	void get_cutting_pos(uint32_t n, uint32_t m, uint32_t offset, uint32_t step_size, std::vector<uint32_t> & cut_pos, Cut** cut_matrix, Params & params);

	void populate_prioritized_keys(uint32_t best_in_mem_entries, std::vector<uint32_t> & cut_pos, std::vector<std::string> & keys, std::vector<std::pair<uint32_t, uint32_t> > & key_multiplicity_to_be_sorted, std::unordered_map<std::string, uint16_t> & partitioned_keys, std::unordered_set<std::string> & in_memory_keys);
	std::pair<uint32_t, uint32_t> get_partitioned_keys(std::vector<std::string> & _keys, std::vector<uint32_t> & _key_multiplicity, std::unordered_map<std::string, uint16_t> & partitioned_keys, std::unordered_set<std::string> & in_memory_keys, bool & turn_on_NBJ, bool appr_flag); // return the number of partitions
	void get_emulated_cost_MatrixDP();
	void get_emulated_cost_MatrixDP(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	void get_emulated_cost_ApprMatrixDP();
	void get_emulated_cost_ApprMatrixDP(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	// key2RValue stores the in-memory partition from relation R
	void partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::unordered_set<std::string> & in_memory_keys, std::unordered_map<std::string, std::string> & key2Rvalue, uint32_t num_pre_partitions, uint32_t num_random_in_mem_partitions, std::string file_name, uint32_t entry_size,uint32_t num_entries, uint32_t divider, double selection_ratio, uint64_t* selection_seed, std::string prefix, uint32_t depth, uint32_t filter_condition, bool build_in_mem_partition_flag=false);
	void load_key_multiplicity(std::vector<std::string> & _keys, std::vector<uint32_t> & _key_multiplicity, bool partial = false);
	void extract_conditions_tpch_q12_query(); 
	bool is_qualified_for_condition(const std::string & entry, uint32_t filter_condition);
	uint64_t get_hash_value(std::string & key, HashType & ht, uint32_t seed);

	static void print_counter_histogram( const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::vector<uint32_t> & key_multiplicity, const std::vector<std::string> & keys, uint32_t num_partitions);
	static uint32_t s_seed;
        static void get_key_string(const std::string & raw_str, std::string & result_string, ATTRIBUTE_TYPE & key_type, uint16_t key_size);

	// solving ax^2 + bx + c = 0 for DHH
	static uint32_t est_best_num_partitions(uint32_t & num_of_in_memory_partitions, uint32_t & num_of_random_in_memory_entries, double a, double b, double c, double hashtable_fulfilling_percent);
    static uint32_t get_hash_map_size(uint32_t k, uint32_t key_size, uint8_t size_of_partitionID=2);
    static uint32_t get_max_hash_map_entries(uint32_t num_pages, uint32_t key_size, uint8_t size_of_partitionID=2);
};



#endif
