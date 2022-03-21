#ifndef ESTIMATOR_H
#define ESTIMATOR_H

#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <chrono>

#include "parameters.h"

class Estimator {
	std::vector<std::string> keys_;
	std::vector<uint32_t> key_multiplicity_;
	Params params_;
public:
	double cpu_duration = 0; 
	double io_duration = 0; 
        uint32_t left_entries_per_page;
        uint32_t right_entries_per_page;
	std::vector<uint32_t> repartitioned_keys;
	Estimator(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params);
	uint64_t get_estimated_io();
	uint64_t get_estimated_io_BNLJ(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth);
	uint64_t get_estimated_io_HP(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages,uint8_t depth);
	uint64_t get_estimated_io_MatrixDP(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth);
	uint64_t get_estimated_io_Hybrid(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth);

	uint64_t get_estimated_io_BNLJ(std::vector<uint32_t> & idxes, uint8_t depth=0U){
	    return get_estimated_io_BNLJ(idxes, params_.B, depth);
	}
	uint64_t get_estimated_io_HP(std::vector<uint32_t> & idxes,uint8_t depth=0U){
	    return get_estimated_io_HP(idxes,params_.B, depth);
	}
	uint64_t get_estimated_io_MatrixDP(std::vector<uint32_t> & idxes, uint8_t depth=0U){
	    return get_estimated_io_MatrixDP(idxes, params_.B, depth);
	}
	uint64_t get_estimated_io_Hybrid(std::vector<uint32_t> & idxes, uint8_t depth=0U){
	    return get_estimated_io_Hybrid(idxes, params_.B, depth);
	}
	
	static uint32_t s_seed;
	static uint64_t get_hash_value(std::string & key, HashType & ht, uint32_t seed);
	static void get_partitioned_keys(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::unordered_map<std::string, uint32_t> & partitioned_keys, std::vector<std::vector<uint32_t> > & partitions, Params & params, uint8_t depth);
};



#endif
