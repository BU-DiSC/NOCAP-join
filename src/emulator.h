#ifndef EMULATOR_H
#define EMULATOR_H

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <functional>

#include "parameters.h"

class Emulator {
	Params params_;
public:
	uint32_t left_entries_per_page;
	uint32_t right_entries_per_page;
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
	uint32_t output_write_cnt = 0;

	int join_output_fd = -1;
	char* join_output_buffer;
	uint32_t join_output_offset = 0;
	uint32_t join_entry_size;
        uint32_t join_output_entries_counter = 0;
        uint32_t join_output_entries_per_page = 0;
	bool opened = false;
	bool rounded_hash = false;

	Emulator(Params & params);

	void write_and_clear_one_page(int fd, char* src);
	void add_one_record_into_join_result_buffer(const char* src1, size_t len1, const char* src2, size_t len2);
	ssize_t read_one_page(int fd, char* src);
	void finish();

        void clct_partition_stats();	

	void get_emulated_cost();

	// Nested Block Join
	void get_emulated_cost_NBJ();
	void get_emulated_cost_NBJ(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, bool hash = false);

	// Sort Merge Join
	template <typename T> void internal_sort(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_entries, std::vector<uint32_t> & num_entries_per_run);
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
	uint32_t get_partitioned_keys(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::unordered_map<std::string, uint16_t> & partitioned_keys, std::unordered_set<std::string> & top_matching_keys, Params & params, bool appr_flag); // return the number of partitions
	void get_emulated_cost_MatrixDP();
	void get_emulated_cost_MatrixDP(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	void get_emulated_cost_ApprMatrixDP();
	void get_emulated_cost_ApprMatrixDP(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	void partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::unordered_set<std::string> & top_matching_keys, uint32_t num_pre_partitions, std::string file_name, uint32_t entry_size,uint32_t num_entries, uint32_t divider, std::string prefix, uint32_t depth);
	void load_key_multiplicity(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, bool partial = false);

	static void print_counter_histogram( const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::vector<uint32_t> & key_multiplicity, const std::vector<std::string> & keys, uint32_t num_partitions);
	static uint32_t s_seed;
	uint64_t get_hash_value(std::string & key, HashType & ht, uint32_t seed);
        static uint32_t get_hash_map_size(uint32_t k, uint32_t key_size, uint8_t size_of_partitionID=2);
        static uint32_t get_hash_map_step_size(uint32_t key_size, uint8_t size_of_partitionID=2);
};



#endif
