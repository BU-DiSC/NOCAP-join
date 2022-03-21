#ifndef EMULATOR_H
#define EMULATOR_H

#include <unordered_map>

#include "parameters.h"

class Emulator {
	Params params_;
public:
	uint32_t left_entries_per_page;
	uint32_t right_entries_per_page;
	double join_duration = 0;
	double probe_duration = 0;
	double partition_duration = 0;
	double algo_duration = 0;
	double partition_cpu_duration = 0;
	double partition_io_duration = 0;
	double io_duration = 0;
	double tmp_duration = 0;
	uint32_t tmp_cnt = 0;
	uint32_t output_cnt = 0;
	uint32_t read_cnt = 0;
	uint32_t write_cnt = 0;
	uint32_t random_io_cnt = 0;
	uint32_t random_load_in_probe_cnt = 0;

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

	void get_emulated_cost();
	void get_emulated_cost_BNLJ();
	void get_emulated_cost_BNLJ(std::string left_file_name, std::string right_file_name, bool hash = false);
	void get_emulated_cost_HP();
	void get_emulated_cost_HP(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_um_entries, uint32_t depth);
	void get_emulated_cost_DHH();
	void get_emulated_cost_DHH(std::string left_file_name, std::string right_file_name, uint32_t depth);
	void get_emulated_cost_MatrixDP();
	void get_emulated_cost_MatrixDP(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth);
	void get_emulated_cost_ApprMatrixDP();// ApprMatrixDP not implemented yet
	void partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint32_t> & partitioned_keys, uint32_t num_pre_partitions, std::string file_name, uint32_t entry_size,uint32_t divider, std::string prefix, uint32_t depth);
	void load_key_multiplicity(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, bool partial = false);

	static void print_counter_histogram(std::vector< std::vector<uint32_t> > & partitions, std::vector<uint32_t> & key_multiplicity);
};

#endif
