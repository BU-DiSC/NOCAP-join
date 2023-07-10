#include "parameters.h"
#include "schema.h"
#include "emulator.h"

#include <cmath>
#include <iostream>
#include <vector>
#include <deque>
#include <fstream>
#include <string>
#include <unordered_map>
#include <queue> 
#include <cstring>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "hash/md5.h"
#include "hash/murmurhash.h"
#include "hash/Crc32.h"
#include "hash/sha-256.h"
#include "hash/xxhash.h"
#include "hash/citycrc.h"
#include "hash/robin_hood.h"
#include <functional>



uint32_t Emulator::s_seed = 0xbc9f1d34;
Emulator::Emulator(Params & params){
    params_ = params;
    rounded_hash = params.rounded_hash;
    join_duration = 0;
    io_duration = 0 ;
    partition_duration = 0;
    read_duration = 0;
    output_duration = 0;
    left_entries_per_page = floor(DB_PAGE_SIZE/params_.left_E_size);
    right_entries_per_page = floor(DB_PAGE_SIZE/params_.right_E_size);
    left_selection_seed = params.left_selection_seed;
    right_selection_seed = params.right_selection_seed;
    step_size = floor(left_entries_per_page*floor((params_.B - 1 - params_.NBJ_outer_rel_buffer)*1.0/FUDGE_FACTOR));

    join_entry_size = params_.left_E_size + params_.right_E_size - params_.join_key_size;
    join_output_entries_per_page = DB_PAGE_SIZE/join_entry_size;

    selection_dist = std::uniform_real_distribution<double>(0.0, 1.0);
}

void Emulator::print_counter_histogram(const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::vector<uint32_t> & key_multiplicity, const std::vector<std::string> & keys, uint32_t num_partitions){
    std::unordered_map<std::string, uint32_t> key2multiplicity;
    for(uint32_t i = 0; i < keys.size(); i++){
    key2multiplicity[keys[i]] = key_multiplicity[i];
    }
    std::vector<std::vector<std::string> > partitions (num_partitions, std::vector<std::string> ());
    for(auto it = partitioned_keys.begin(); it != partitioned_keys.end(); it++){
        partitions[it->second].push_back(it->first);    
    }

    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t> > histogram;
    std::unordered_map<uint32_t, uint32_t> counter;
    uint32_t count = 0;
    uint32_t multiplicity = 0;
    for(uint32_t i = 0; i < partitions.size(); i++){
    count = partitions[i].size();
        if(histogram.find(count) == histogram.end()){
        histogram[count] = std::unordered_map<uint32_t, uint32_t> ();
    }
    if(counter.find(count) == counter.end()){
        counter[count] = 0;
    }
    counter[count]++;
    for(auto & key: partitions[i]){
        multiplicity = key2multiplicity[key];
        if(histogram[count].find(multiplicity) == histogram[count].end()){
        histogram[count][multiplicity] = 0;
        }
        histogram[count][multiplicity]++;
    }
    
    }
    
    for(auto const & x:histogram){
    std::cout << x.first << " : " << counter[x.first] << "\t";
    for(auto const & y: x.second){
        std::cout << y.first << "-" << y.second << " ";
    }
    std::cout << std::endl;
    }
}

void Emulator::clct_partition_stats(){
    /* Collect the partitioning statistics 
     * Not fully implemented for rounded hash between SMJ and GHJ
     */
    if(params_.pjm == NBJ || params_.pjm == DynamicHybridHash || params_.pjm == SMJ){
        return;
    }

    params_.debug = true;
    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity;
    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_with_partition_size_to_be_sorted;
    std::unordered_map<std::string, uint16_t> partitioned_keys;
    std::unordered_set<std::string> in_memory_keys; 
    std::vector<uint32_t> counter = std::vector<uint32_t> (params_.num_partitions, 0);
    load_key_multiplicity(keys, key_multiplicity);
    uint64_t hash_value;
    uint32_t partition_id;
    uint64_t divider = params_.left_table_size*params_.left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent);
    bool turn_on_NBJ = false;
    if(params_.pjm == GHJ){
    if(ceil(params_.left_table_size*params_.left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent)/params_.num_partitions) - ceil(params_.left_table_size*params_.left_selection_ratio/params_.num_partitions/step_size) >= 1 || ceil(params_.left_table_size*params_.left_selection_ratio/params_.num_partitions/step_size) >= 2 + params_.randwrite_seqread_ratio ){
        divider = params_.num_partitions;
    }
    for(std::string & key: keys){
            hash_value = get_hash_value(key, params_.ht, s_seed);
        if(params_.rounded_hash){
                partition_id = (hash_value%divider)%params_.num_partitions;
        }else{
                partition_id = hash_value%params_.num_partitions;
        }
        partitioned_keys[key] = partition_id;
        counter[partition_id]++;
    }

        
    }else if(params_.pjm == MatrixDP){
        get_partitioned_keys(keys, key_multiplicity, partitioned_keys, in_memory_keys, turn_on_NBJ, false);
        for(auto it = partitioned_keys.begin(); it != partitioned_keys.end(); it++){
            counter[it->second]++;
        }    
    }else if(params_.pjm == ApprMatrixDP){
        keys.clear();
        key_multiplicity.clear();
        load_key_multiplicity(keys, key_multiplicity, true);
        std::pair<uint32_t, uint32_t> partition_num_result = get_partitioned_keys(keys, key_multiplicity, partitioned_keys, in_memory_keys, turn_on_NBJ, true);
        uint32_t num_pre_partitions = partition_num_result.first;
        uint32_t num_random_in_mem_partitions = partition_num_result.second;
        uint32_t num_remaining_entries = 0;
        uint32_t num_partitions = 0;
        keys.clear();
        key_multiplicity.clear();
        load_key_multiplicity(keys, key_multiplicity, false);

        num_remaining_entries = params_.left_table_size - partitioned_keys.size();
        num_partitions = params_.num_partitions - num_pre_partitions - get_hash_map_size(partitioned_keys.size(), params_.join_key_size, 2);
        divider = ceil(num_remaining_entries*params_.left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent)); 
        if(ceil(num_remaining_entries*params_.left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent)/num_partitions) - ceil(num_remaining_entries*params_.left_selection_ratio/params_.num_partitions/step_size) >= 1 || ceil(num_remaining_entries*params_.left_selection_ratio/params_.num_partitions/step_size) >= 2 + params_.randwrite_seqread_ratio ){
            divider = num_partitions;
        }
        if (in_memory_keys.size() > 0) {
            for(auto it = in_memory_keys.begin(); it != in_memory_keys.end(); it++){
                partitioned_keys[*it] = 0;
                counter[0]++;
            }
            for(auto it = partitioned_keys.begin(); it != partitioned_keys.end(); it++){
                counter[it->second+1]++;
            }
        } else {
            for(auto it = partitioned_keys.begin(); it != partitioned_keys.end(); it++){
                counter[it->second]++;
            }
        }
        
        
        for(std::string & key: keys){
            if(partitioned_keys.find(key) == partitioned_keys.end() && in_memory_keys.find(key) == in_memory_keys.end()){
                hash_value = get_hash_value(key, params_.ht, s_seed);
                if(params_.rounded_hash){
                    partition_id = (hash_value%divider)%num_partitions + num_pre_partitions;
                }else{
                    partition_id = hash_value%num_partitions + num_pre_partitions;
                }
                partitioned_keys[key] = partition_id;
                counter[partition_id]++;
            }
        }
    }

    for(uint32_t i = 0; i < key_multiplicity.size(); i++){
        if(partitioned_keys.find(keys[i]) == partitioned_keys.end()){
            key_multiplicity_with_partition_size_to_be_sorted.push_back(std::make_pair(key_multiplicity[i], 0));
        }else{
            key_multiplicity_with_partition_size_to_be_sorted.push_back(std::make_pair(key_multiplicity[i], counter[partitioned_keys[keys[i]]]));
        }
    }
    std::sort(key_multiplicity_with_partition_size_to_be_sorted.begin(), key_multiplicity_with_partition_size_to_be_sorted.end(), [](const std::pair<uint32_t, uint32_t> & a, const std::pair<uint32_t, uint32_t> & b){    return a.first < b.first || (a.first == b.first && a.second > b.second);
            });
       

    std::ofstream fp;
    fp.open(params_.part_stats_path.c_str());
    for(uint32_t i = 0; i < key_multiplicity_with_partition_size_to_be_sorted.size(); i++){
        fp << key_multiplicity_with_partition_size_to_be_sorted[i].first << "," << key_multiplicity_with_partition_size_to_be_sorted[i].second << std::endl;
    }
    fp.close();

}

bool Emulator::is_qualified_for_condition(const std::string & entry, uint32_t filter_condition) {
    switch(filter_condition) {
    case 0:
        return true;
    case 1: // lineitem filter condition in Q12
        {
            Date l_shipdate (entry.substr(66, 10));
            Date l_commitdate (entry.substr(76, 10));
            Date l_receiptdate (entry.substr(86, 10));
            std::string l_shipmode = entry.substr(121, 10);
            if (!(l_commitdate < l_receiptdate)) return false;
            //if (!(l_shipdate < l_commitdate)) return false;
            /*
            if ((!TPCH_Q12_YEAR_ROUGHLY_MATCH && l_receiptdate.year != tpch_q12_required_year) ||
                (TPCH_Q12_YEAR_ROUGHLY_MATCH && (l_receiptdate.year < tpch_q12_required_year ||
                   (int)(l_receiptdate.year) - tpch_q12_required_year > TPCH_Q12_YEAR_OFFSET))) return false;
            
            for (size_t i = 0; i < tpch_q12_results.size(); i++) {
                    if (l_shipmode.find(std::get<0>(tpch_q12_results[i])) != std::string::npos) {
                    return true;
                    }
            }
            return false;
            */
            return true;
        }
    default:
        break;
    }
    return true;

}

uint64_t Emulator::get_hash_value(std::string & key, HashType & ht, uint32_t seed){
    string input_key = key; 
    uint64_t result = 0;
    switch(ht){
        case MD5:
            memcpy(&result, md5(input_key).c_str(), sizeof(result));
            break;
        case SHA2:{
            uint8_t hash[32];
            const uint8_t * a = (const uint8_t*)input_key.c_str();
            calc_sha_256(hash, a, input_key.length());
            for(int i = 0; i < 32; i++){
               result = (result << 1) + (hash[i]&0x1);
            }
            break;
        }

        case MurMurhash: {
            result = MurmurHash64A( input_key.c_str(), input_key.size(), seed);
            break;
        }
        case RobinHood: {
        result = robin_hood::hash_bytes(input_key.c_str(), input_key.size());
            break;
        }
        case XXhash:
        {
            // result = MurmurHash2(key.c_str(), key.size(), seed);
            XXH64_hash_t const p = seed;
            const void * key_void = input_key.c_str();
            XXH64_hash_t const h = XXH64(key_void, input_key.size(), p);
            result = h;
            break;
        }
        case CRC:{
            const void * key_void = input_key.c_str();
            result = crc32_fast( key_void, (unsigned long)input_key.size(), seed );
            break;
        }
        case CITY:{
            const char * key_void = input_key.c_str();
            result = CityHash64WithSeed( key_void, (unsigned long)input_key.size(), (unsigned long) seed);
            break;
        }
        default:
            result = MurmurHash2(input_key.c_str(), input_key.size(), seed);
            break;
    }

    return result;
}

uint32_t Emulator::get_hash_map_size(uint32_t k, uint32_t key_size, uint8_t size_of_partitionID){
    uint32_t dividend = k*FUDGE_FACTOR*(key_size + size_of_partitionID);
    if(dividend%DB_PAGE_SIZE == 0){
        return dividend/DB_PAGE_SIZE;
    }else{
        return dividend/DB_PAGE_SIZE + 1;
    }
}

inline uint32_t Emulator::get_max_hash_map_entries(uint32_t num_pages, uint32_t key_size, uint8_t size_of_partitionID){
    return floor(DB_PAGE_SIZE*num_pages/(FUDGE_FACTOR*(key_size + size_of_partitionID)));
}

inline void Emulator::write_and_clear_one_page(int fd, char* src){ // assume the file descriptor is correctly opened
    std::chrono::time_point<std::chrono::high_resolution_clock> io_start = std::chrono::high_resolution_clock::now();
    write(fd, src, DB_PAGE_SIZE);
    std::chrono::time_point<std::chrono::high_resolution_clock> io_end = std::chrono::high_resolution_clock::now();
    io_duration += (std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start)).count();
    write_cnt++;
    memset(src, 0, DB_PAGE_SIZE); 
}

inline void Emulator::add_one_record_into_join_result_buffer(const char* src1, size_t len1, const char* src2, size_t len2){
    if(!opened){

        posix_memalign((void**)&join_output_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
        memset(join_output_buffer, 0, DB_PAGE_SIZE);
	if (!params_.no_join_output) {
	    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
            int write_flags = O_RDWR | O_TRUNC | O_CREAT;
            if(!params_.no_direct_io){
                write_flags |= O_DIRECT;
            }
            if(!params_.no_sync_io){
                write_flags |= O_SYNC;
            }
            join_output_fd = open(params_.output_path.c_str(),write_flags,write_mode);
	}
        
        opened = true;
    }

    output_cnt++;
    memcpy(join_output_buffer + join_output_offset, src1, len1);
    join_output_offset += len1;
    memcpy(join_output_buffer + join_output_offset, src2, len2);
    join_output_offset += len2;

    if (params_.tpch_q12_flag && tpch_q12_results != nullptr) {
        std::string o_orderpriority = std::string(src2 + 35, 15);
        std::string l_shipmode = std::string(src1 + 121, 10);
        for (size_t i = 0; i < tpch_q12_results->size(); i++) {
           if(l_shipmode.find(std::get<0>(tpch_q12_results->at(i))) != std::string::npos) {
        if (o_orderpriority.find("1-URGENT") != std::string::npos || o_orderpriority.find("2-HIGH") != std::string::npos) {
            std::get<1>(tpch_q12_results->at(i)) = std::get<1>(tpch_q12_results->at(i)) + 1;

            } else {
            std::get<2>(tpch_q12_results->at(i)) = std::get<2>(tpch_q12_results->at(i)) + 1;
        }
           }           
        } 
    }
    double tmp_duration1;
    double tmp_duration2;
    if(join_output_offset + (len1 + len2) > DB_PAGE_SIZE){
        if(!params_.no_join_output){
            tmp_duration1 = io_duration;
            write_and_clear_one_page(join_output_fd, join_output_buffer);
            tmp_duration2 = io_duration;
            output_duration += tmp_duration2 - tmp_duration1;
            output_write_cnt++;
        }else{
            memset(join_output_buffer, 0, DB_PAGE_SIZE);
        }
    join_output_offset = 0;
    }
}

inline void Emulator::finish(){
    
    if(join_output_offset > 0 && !params_.no_join_output){
	if (join_output_fd == -1) {
	    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
            int write_flags = O_RDWR | O_TRUNC | O_CREAT;
            if(!params_.no_direct_io){
                write_flags |= O_DIRECT;
            }
            if(!params_.no_sync_io){
                write_flags |= O_SYNC;
            }
            join_output_fd = open(params_.output_path.c_str(),write_flags,write_mode);
	    opened = true;
	}
        write_and_clear_one_page(join_output_fd, join_output_buffer);
        output_write_cnt++;
        memset(join_output_buffer, 0, DB_PAGE_SIZE);
    }

    if(opened){
        fsync(join_output_fd);
        close(join_output_fd);
        delete[] join_output_buffer;
        opened = false;
    }
}

inline ssize_t Emulator::read_one_page(int fd, char* src){ // assume the file descriptor is correctly opened
    ssize_t read_bytes = 0;
    memset(src, 0, DB_PAGE_SIZE); 
    std::chrono::time_point<std::chrono::high_resolution_clock> io_start = std::chrono::high_resolution_clock::now();
    read_bytes = read(fd, src, DB_PAGE_SIZE);
    std::chrono::time_point<std::chrono::high_resolution_clock> io_end = std::chrono::high_resolution_clock::now();
    io_duration += (std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start)).count();
    read_duration += (std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start)).count();
    read_cnt++;
    return read_bytes;
}

void Emulator::load_key_multiplicity(std::vector<std::string> & _keys, std::vector<uint32_t> & _key_multiplicity, bool partial){
    _keys.clear();
    _key_multiplicity.clear();
    std::ifstream fp;
    fp.open(params_.workload_dis_path.c_str(), std::ios::in);
    //std::cout << "Using " << params.workload_path << std::endl;
    fp >> params_.left_table_size >> params_.right_table_size >> params_.join_key_type >> params_.join_key_size >> params_.left_E_size >> params_.right_E_size;
    if(partial){
        _keys = std::vector<std::string> (params_.k, "");
        _key_multiplicity = std::vector<uint32_t> (params_.k, 0);
        for(auto i = 0; i < params_.k; i++){
            fp >> _keys[i] >> _key_multiplicity[i];
        }
    }else{
        _keys = std::vector<std::string> (params_.left_table_size, "");
        _key_multiplicity = std::vector<uint32_t> (params_.left_table_size, 0);
        for(auto i = 0; i < params_.left_table_size; i++){
            fp >> _keys[i] >> _key_multiplicity[i];
        }
    } 
    
    fp.close();
}

void Emulator::extract_conditions_tpch_q12_query() {
    std::ifstream fp;
    fp.open(params_.tpch_q12_path.c_str(), std::ios::in);
    std::string line;
    size_t pos;
    size_t next_pos;
    tpch_q12_results = new std::vector<std::tuple<std::string, uint32_t, uint32_t> > ();
    while (getline(fp, line)) {
    pos = line.find("l_shipmode");
    while(pos != std::string::npos) {
       pos = line.find("'", pos);
       if (pos == std::string::npos) continue;
       next_pos = line.find("'", pos + 1);
       tpch_q12_results->emplace_back(line.substr(pos+1, next_pos - pos - 1), 0U, 0U);
       //std::cout << std::get<0>(tpch_q12_results.back()) << std::endl;
       pos = next_pos + 1;
    }
    pos = line.find("l_receiptdate >= date");
    if(pos != std::string::npos) {
       pos = line.find("'", pos);
       if(pos == std::string::npos) continue;
       tpch_q12_required_year = atoi(line.substr(pos+1, 4).c_str());
       //std::cout << tpch_q12_required_year << std::endl;
    }
    }
    fp.close();
}

void Emulator::get_emulated_cost(){
    if(params_.clct_part_stats_only_flag){
        clct_partition_stats();
        return;    
    }
    if(params_.tpch_q12_flag) {
        extract_conditions_tpch_q12_query();
    }
    system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
    system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");

    load_key_multiplicity(keys, key_multiplicity, true);
    switch(params_.pjm){
        case NBJ:
            get_emulated_cost_NBJ();
            break;
        case GHJ:
            get_emulated_cost_GHJ();
            break;
        case MatrixDP:
            get_emulated_cost_MatrixDP();
            break;
        case DynamicHybridHash:
            get_emulated_cost_DHH();
            break;
        case SMJ:
            get_emulated_cost_SMJ();
            break;
        case ApprMatrixDP:
            get_emulated_cost_ApprMatrixDP();
        default:
            break;
    }

    if (params_.tpch_q12_flag) {
        std::sort(tpch_q12_results->begin(), tpch_q12_results->end());
        std::cout << "l_shipmode|high_line_count|low_line_count" << std::endl;
        for (size_t i = 0; i < tpch_q12_results->size(); i++) {
        std::cout << std::get<0>(tpch_q12_results->at(i)) << "|";
        std::cout << std::get<1>(tpch_q12_results->at(i)) << "|";
        std::cout << std::get<2>(tpch_q12_results->at(i)) << std::endl;
        }
	if (tpch_q12_results != nullptr) {
	    delete tpch_q12_results;
	}
    }
}

void Emulator::get_emulated_cost_NBJ(){
    
    // only set filter condition of S for now in case of tpch Q12 query 
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = params_.tpch_q12_flag ? 1 : 0;

    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_NBJ(params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0, R_filter_condition, S_filter_condition, true);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
    probe_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}

void Emulator::get_emulated_cost_NBJ(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth, uint32_t left_filter_condition, uint32_t right_filter_condition, bool hash){
    uint32_t left_value_size = params_.left_E_size - params_.join_key_size;
    uint32_t right_value_size = params_.right_E_size - params_.join_key_size;
    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
        read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int fd_R = open(left_file_name.c_str(), read_flags,read_mode ); 
    int fd_S = open(right_file_name.c_str(), read_flags,read_mode ); 

    bool end_flag_R = false;
    bool end_flag_S = false;
    uint32_t R_num_pages_in_buff = 0;
    ssize_t read_bytes_R = 0;
    ssize_t read_bytes_S = 0;
    size_t S_remainder = 0;
    size_t R_remainder = 0;
    uint32_t read_R_entries = 0;
    uint32_t read_S_entries = 0;
    int cmp_result = 0;
    char* src1_addr = nullptr;
    char* src1_end_addr = nullptr;
    char* src2_addr = nullptr;
    char* src2_end_addr = nullptr;

    char* tmp_buffer;
    std::string tmp_key;
    std::string tmp_input_key;
    std::string tmp_entry;
    std::unordered_map<std::string, std::string>* key2Rvalue = nullptr;
    uint16_t num_passes = 0U;

    std::mt19937 left_selection_generator (left_selection_seed); 
    std::mt19937 right_selection_generator (right_selection_seed); 
    std::chrono::time_point<std::chrono::high_resolution_clock>  io_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  io_end;

    uint32_t inner_relation_pages;
    if (hash) {
        // We cannot fully use (params_.B - 1 - params_.NBJ_outer_rel_buffer) pages to load data because of the FUDGE_FACTOR when building the hash table
        inner_relation_pages = max(min((uint32_t)floor(1.0*(params_.B - 1 - params_.NBJ_outer_rel_buffer)/FUDGE_FACTOR), (uint32_t)ceil(ceil(left_num_entries*1.0/floor(DB_PAGE_SIZE*1.0/params_.left_E_size))*FUDGE_FACTOR) + 1), 1U);
    } else {
        inner_relation_pages = max(min((params_.B - 1 - params_.NBJ_outer_rel_buffer), (uint32_t)ceil(left_num_entries/floor(DB_PAGE_SIZE*1.0/params_.left_E_size)) + 1), 1U);
    }

     posix_memalign((void**)&S_rel_buffer,DB_PAGE_SIZE,params_.NBJ_outer_rel_buffer*DB_PAGE_SIZE);
     posix_memalign((void**)&R_rel_buffer,DB_PAGE_SIZE,inner_relation_pages*DB_PAGE_SIZE);

    while(true){
       
        //auto tmp_start = std::chrono::high_resolution_clock::now();
        src2_end_addr = R_rel_buffer;
        R_num_pages_in_buff = 0;
        memset(R_rel_buffer, 0, inner_relation_pages*DB_PAGE_SIZE);
        num_passes++;
        for(uint32_t i = 0; i < inner_relation_pages; i++){
            read_bytes_R = read_one_page(fd_R, R_rel_buffer + i*DB_PAGE_SIZE);
            if(read_bytes_R <= 0){
                end_flag_R = true;
                break;
            }
            R_num_pages_in_buff++;
            src2_end_addr += DB_PAGE_SIZE;
        }

    
        if(hash){
	    key2Rvalue = new std::unordered_map<std::string, std::string>();
            key2Rvalue->clear(); 

            for(uint64_t i = 0; i < inner_relation_pages  && i < R_num_pages_in_buff ; i++){
                tmp_buffer = R_rel_buffer + i*DB_PAGE_SIZE;
                //for(auto j = 0; j < left_entries_per_page && read_R_entries < left_num_entries; j++){
                for(uint64_t j = 0; j < left_entries_per_page; j++){
                    //if(*(tmp_buffer+j*params_.left_E_size) == '\0') break;
                    //if(depth > 0 && read_R_entries >= left_num_entries) break;
                    if(read_R_entries >= left_num_entries) {
                        end_flag_R = true;
                        break;
                    }
                    ByteArray2String(std::string(tmp_buffer + j*params_.left_E_size, params_.join_key_size), tmp_key, params_.join_key_type, params_.join_key_size);
                    tmp_entry = std::string(tmp_buffer + j*params_.left_E_size, params_.left_E_size);
                    read_R_entries++;
                    if (depth == 0 && left_filter_condition == 0 && !params_.tpch_q12_flag && params_.left_selection_ratio < 1.0 && selection_dist(left_selection_generator) >= params_.left_selection_ratio) continue; 
                                    if (depth == 0 && params_.tpch_q12_flag && left_filter_condition > 0) {
                       if(!is_qualified_for_condition(std::string(tmp_buffer + j*params_.left_E_size, params_.left_E_size), left_filter_condition)) {
                        continue;
                       }
                    } 
                    key2Rvalue->emplace(tmp_key, tmp_entry);
                }
            }

            /*
            if(read_R_entries >= left_num_entries) {
                end_flag_R = true;
                break;
            }*/
        }

        if(read_R_entries == left_num_entries) {
            end_flag_R = true;
        }

        lseek(fd_S, 0, SEEK_SET);
        end_flag_S = false; 
        uint32_t k = 0;
        uint32_t S_local_counter = 0;
        uint32_t R_local_counter = 0;


        while(true){
            S_local_counter = 0;
            memset(S_rel_buffer, 0, params_.NBJ_outer_rel_buffer*DB_PAGE_SIZE);
            src1_end_addr = S_rel_buffer;
            for(auto i = 0; i < params_.NBJ_outer_rel_buffer; i++){
                read_bytes_S = read_one_page(fd_S, S_rel_buffer + i*DB_PAGE_SIZE);
                src1_end_addr += DB_PAGE_SIZE;
                if(read_bytes_S <= 0){
                    end_flag_S = true; 
                    break;
                }
            }

            // start nested loop join
            src1_addr = S_rel_buffer;
            while(S_local_counter < params_.NBJ_outer_rel_buffer*right_entries_per_page && src1_end_addr - src1_addr > 0 && read_S_entries < right_num_entries){
                if (!(depth == 0 && right_filter_condition == 0 && !params_.tpch_q12_flag && params_.right_selection_ratio < 1.0 && selection_dist(right_selection_generator) >= params_.right_selection_ratio) && 
                    !(depth == 0 && right_filter_condition > 0 && params_.tpch_q12_flag && !is_qualified_for_condition(std::string(src1_addr, params_.right_E_size), right_filter_condition))) {
                    if(hash){
                        ByteArray2String(std::string(src1_addr, params_.join_key_size), tmp_key, params_.join_key_type, params_.join_key_size);
                        
                        if(key2Rvalue->find(tmp_key) != key2Rvalue->end()){
                            add_one_record_into_join_result_buffer(src1_addr, params_.right_E_size, key2Rvalue->at(tmp_key).c_str(), params_.left_E_size);
                        } 
                    }else{
                        src2_addr = R_rel_buffer;
                        R_local_counter = 0;
                        uint32_t upper_bound = left_entries_per_page*inner_relation_pages;
                        if(R_num_pages_in_buff < inner_relation_pages){
                            upper_bound = left_num_entries%upper_bound;
                        }
                        while(R_local_counter < upper_bound && src2_end_addr - src2_addr > 0){
                            cmp_result = memcmp(src1_addr, src2_addr, params_.join_key_size);
                            if(cmp_result == 0){
                                add_one_record_into_join_result_buffer(src1_addr, params_.right_E_size, src2_addr, params_.left_E_size);
                            }
                            src2_addr += params_.left_E_size;
                            R_remainder = (int)(src2_addr - R_rel_buffer)%DB_PAGE_SIZE;
                            if(DB_PAGE_SIZE - R_remainder < params_.left_E_size){
                                src2_addr += DB_PAGE_SIZE - R_remainder;
                            }
                            R_local_counter++;
                        }
                    }
                }
                src1_addr += params_.right_E_size;
                S_local_counter++;
                read_S_entries++;

                S_remainder = (int)(src1_addr - S_rel_buffer)%DB_PAGE_SIZE;
                if(DB_PAGE_SIZE - R_remainder < params_.right_E_size){
                    src1_addr += DB_PAGE_SIZE - S_remainder;
                }
            }
            if(end_flag_S) break;

        }
        read_S_entries = 0;

	if (key2Rvalue != nullptr) {
	    key2Rvalue->clear();
            delete key2Rvalue;
	    key2Rvalue = nullptr;
	}
        if(end_flag_R) break;
    }
    if (params_.debug) {
        std::cout << "num of passes : " << num_passes << std::endl;
    }
    //std::cout << right_num_entries << "-" << output_cnt << std::endl;
    memset(S_rel_buffer, 0, DB_PAGE_SIZE*params_.NBJ_outer_rel_buffer);
    memset(R_rel_buffer, 0, DB_PAGE_SIZE*inner_relation_pages);
    free(R_rel_buffer);
    free(S_rel_buffer);
    close(fd_R);
    close(fd_S);
}

template <typename T> void Emulator::internal_sort(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_entries, double selection_ratio, uint64_t* selection_seed, uint32_t filter_condition, std::vector<uint32_t> & num_entries_per_run){
    
    std::mt19937 selection_generator (*selection_seed); 

    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
        read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    if(!params_.no_direct_io){
        write_flags |= O_DIRECT;
    }
    if(!params_.no_sync_io){
        write_flags |= O_SYNC;
    }
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    uint32_t value_size = entry_size - params_.join_key_size;
    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    char* output_buffer;
    uint32_t output_records_counter_for_one_page = 0;
    uint32_t input_records_counter_for_one_page = entries_per_page;
    uint64_t max_run_size = (uint64_t)(ceil((num_entries*selection_ratio)/entries_per_page)*DB_PAGE_SIZE);
    posix_memalign((void**)&output_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    memset(input_buffer, 0, DB_PAGE_SIZE);
    memset(output_buffer, 0, DB_PAGE_SIZE);

    ssize_t read_bytes = 0;
    uint64_t num_read_entries = 0;
    std::priority_queue<std::pair<std::string, std::string>, std::vector<std::pair<std::string, std::string> >, T  > pq1;
    std::priority_queue<std::pair<std::string, std::string>, std::vector<std::pair<std::string, std::string> >, T > pq2;
    int fd = open(file_name.c_str(), read_flags, read_mode);
    char* tmp_offset = 0;
    std::string tmp_key;
    while(pq1.size() <= (params_.B - 3)*entries_per_page){
        read_bytes = read_one_page(fd, input_buffer);
        if(read_bytes <= 0) break;
        for(auto i = 0; i < entries_per_page; i++){
            tmp_offset = input_buffer + i*entry_size;
            num_read_entries++;

            if (!params_.tpch_q12_flag && filter_condition == 0 && selection_ratio < 1.0 && selection_dist(selection_generator) >= selection_ratio) continue;
            if (params_.tpch_q12_flag && filter_condition > 0 && !is_qualified_for_condition(std::string(tmp_offset, entry_size), filter_condition)) continue;

            ByteArray2String(std::string(tmp_offset, params_.join_key_size), tmp_key, params_.join_key_type, params_.join_key_size);
            pq1.emplace(tmp_key, std::string(tmp_offset + params_.join_key_size, value_size));
        }
    }

    uint32_t run_number = 0;
    uint32_t counter = 0;
    num_entries_per_run.clear();
    std::string tmp_file_name = prefix + "p0-run" + std::to_string(run_number);
    uint32_t output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
    posix_fallocate(output_fd, 0, max_run_size);
    std::string tmp_key1;
    std::string tmp_key2;
    bool end_flag = false;
    while(true){
    
        if(!pq1.empty()){
            tmp_key1 = pq1.top().first;
            tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;
            String2ByteArray(tmp_key1, tmp_offset, params_.join_key_type, params_.join_key_size);
            
            memcpy(tmp_offset + params_.join_key_size, pq1.top().second.c_str(), value_size);
            output_records_counter_for_one_page++;
            if(output_records_counter_for_one_page == entries_per_page){
                counter += entries_per_page;
                seq_write_cnt++;
                write_and_clear_one_page(output_fd, output_buffer);
                output_records_counter_for_one_page = 0U;
            }

            pq1.pop();
        }
        
        if(pq1.empty()){
            pq1.swap(pq2);

            if(output_records_counter_for_one_page > 0){
                counter += output_records_counter_for_one_page;
                seq_write_cnt++;
                write_and_clear_one_page(output_fd, output_buffer);
                output_records_counter_for_one_page = 0U;
            }
            fsync(output_fd);
            ftruncate(output_fd, (off_t)(ceil(counter*1.0/entries_per_page)*DB_PAGE_SIZE));
            close(output_fd);
            num_entries_per_run.push_back(counter);
            counter = 0;
            run_number++;

            tmp_file_name = prefix + "p0-run" + std::to_string(run_number);
            output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
            posix_fallocate(output_fd, 0, max_run_size);
        }

        while(true){
            if(input_records_counter_for_one_page == entries_per_page){
                input_records_counter_for_one_page = 0;
                memset(input_buffer, 0 ,DB_PAGE_SIZE);
                read_bytes = read_one_page(fd, input_buffer);
                if(read_bytes <= 0){
                    end_flag = true;
                    break;
                }
            } 
            tmp_offset = input_buffer + input_records_counter_for_one_page*entry_size;
            input_records_counter_for_one_page++;
            if(num_read_entries >= num_entries) {
                end_flag = true;        
                break;
            }
            num_read_entries++;
            if (!(!params_.tpch_q12_flag && filter_condition == 0 && selection_ratio < 1.0 && selection_dist(selection_generator) >= selection_ratio) &&
                 !(params_.tpch_q12_flag && filter_condition > 0 && !is_qualified_for_condition(std::string(tmp_offset, entry_size), filter_condition))) break;
        }
        if(end_flag) break;
        
        
        ByteArray2String(std::string(tmp_offset, params_.join_key_size), tmp_key2, params_.join_key_type, params_.join_key_size);
        if(tmp_key2 >= tmp_key1){
            pq1.emplace(tmp_key2, std::string(tmp_offset + params_.join_key_size, value_size));
        }else{
            pq2.emplace(tmp_key2, std::string(tmp_offset + params_.join_key_size, value_size));
        } 
    }
    close(fd);
    while(!pq1.empty()){
        tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;    
        String2ByteArray(pq1.top().first, tmp_offset, params_.join_key_type, params_.join_key_size);
        memcpy(tmp_offset + params_.join_key_size, pq1.top().second.c_str(), value_size);
        output_records_counter_for_one_page++;
        if(output_records_counter_for_one_page == entries_per_page){
            counter += entries_per_page;
            seq_write_cnt++;
            write_and_clear_one_page(output_fd, output_buffer);
            output_records_counter_for_one_page = 0U;
        }
        pq1.pop();
    }

    if(output_records_counter_for_one_page > 0){
        counter += output_records_counter_for_one_page;
        seq_write_cnt++;
        write_and_clear_one_page(output_fd, output_buffer);
        output_records_counter_for_one_page = 0U;
    }
    fsync(output_fd);
    ftruncate(output_fd, (off_t)(ceil(counter*1.0/entries_per_page)*DB_PAGE_SIZE));
    close(output_fd);
    num_entries_per_run.push_back(counter);
    if(pq2.empty()) return;

    counter = 0;
    run_number++;
    tmp_file_name = prefix + "p0-run" + std::to_string(run_number);
    output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
    posix_fallocate(output_fd, 0, (off_t)(ceil(pq2.size()*1.0/entries_per_page)*DB_PAGE_SIZE));
    while(!pq2.empty()){
        tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;
        String2ByteArray(pq2.top().first, tmp_offset, params_.join_key_type, params_.join_key_size);
        memcpy(tmp_offset + params_.join_key_size, pq2.top().second.c_str(), value_size);
        output_records_counter_for_one_page++;
        if(output_records_counter_for_one_page == entries_per_page){
            counter +=  entries_per_page;
            seq_write_cnt++;
            write_and_clear_one_page(output_fd, output_buffer);
            output_records_counter_for_one_page = 0U;
        }
        pq2.pop();
    }

    if(output_records_counter_for_one_page > 0){
        counter += output_records_counter_for_one_page;
        seq_write_cnt++;
        write_and_clear_one_page(output_fd, output_buffer);
        output_records_counter_for_one_page = 0U;
    }
    fsync(output_fd);
    close(output_fd);
    num_entries_per_run.push_back(counter);
    return;

}

template <typename T> void Emulator::merge_sort_for_one_pass(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no, std::vector<uint32_t> & num_entries_per_run){
    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
        read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH; 
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    if(!params_.no_direct_io){
        write_flags |= O_DIRECT;
    }
    if(!params_.no_sync_io){
        write_flags |= O_SYNC;
    }
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
    std::vector<int> fd_vec = std::vector<int> (params_.B - 1, -1);
    std::vector<uint32_t> input_records_for_one_page_vec = std::vector<uint32_t> (params_.B - 1, 0);
    std::vector<uint32_t> input_read_pages_vec = std::vector<uint32_t> (params_.B - 1, 0);
    std::vector<uint32_t> last_num_entries_per_merging_run = std::vector<uint32_t> (params_.B - 1, 0);
    std::vector<uint32_t> last_num_entries_per_run = num_entries_per_run;
    num_entries_per_run.clear();
    uint32_t counter = 0;
    uint32_t total_entries = 0;
    for (const uint32_t& tmp_num_entries : num_entries_per_run) {
	total_entries += tmp_num_entries;
    }
    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    uint32_t output_records_counter_for_one_page = 0;
    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,(params_.B-1)*DB_PAGE_SIZE);
    memset(input_buffer, 0, (params_.B-1)*DB_PAGE_SIZE);
    char* output_buffer;
    posix_memalign((void**)&output_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    memset(output_buffer, 0, DB_PAGE_SIZE);

    uint32_t run_idx = 0;
    uint32_t run_inner_idx = 0;
    uint32_t tmp_run_idx = 0;
    std::string prefix_input_filename = prefix + "p" + std::to_string(pass_no) + "-run";
    std::string prefix_output_filename = prefix + "p" + std::to_string(pass_no+1) + "-run";
    std::string tmp_str;
    uint32_t new_run_num = 0;
    uint32_t read_bytes = 0;
    int output_fd;

    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T> pq;

    char* tmp_offset1;
    char* tmp_offset2;
    uint32_t tmp_input_records_for_one_page = 0;
    uint32_t top_run_idx;
    while(run_idx < num_runs){
        for(tmp_run_idx = run_idx; tmp_run_idx - run_idx < params_.B - 1 && tmp_run_idx < num_runs; tmp_run_idx++){
            run_inner_idx = tmp_run_idx - run_idx;
            tmp_str = prefix_input_filename + std::to_string(tmp_run_idx);
            fd_vec[run_inner_idx] = open(tmp_str.c_str(), read_flags, read_mode);
            input_read_pages_vec[run_inner_idx] = 0;
            last_num_entries_per_merging_run[run_inner_idx] = last_num_entries_per_run[tmp_run_idx];
            tmp_offset1 = input_buffer + run_inner_idx*DB_PAGE_SIZE;
            read_bytes = read_one_page(fd_vec[run_inner_idx], tmp_offset1);
            if(read_bytes <= 0){
                close(fd_vec[run_inner_idx]);
                fd_vec[run_inner_idx] = -1;
                memset(tmp_offset1, 0, DB_PAGE_SIZE);
            }else{
                ByteArray2String(std::string(tmp_offset1, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size); 
                pq.emplace(tmp_str, run_inner_idx);
            }
            input_records_for_one_page_vec[run_inner_idx] = 0;
        }
        tmp_str = prefix_output_filename + std::to_string(new_run_num);
        output_fd = open(tmp_str.c_str(), write_flags, write_mode);
	posix_fallocate(output_fd, 0, (off_t)(ceil(total_entries*1.0/entries_per_page)*DB_PAGE_SIZE));

        while(!pq.empty()){
            tmp_offset1 = output_buffer + output_records_counter_for_one_page*entry_size;
            top_run_idx = pq.top().second;
            
            pq.pop();
            tmp_offset2 = input_buffer + top_run_idx*DB_PAGE_SIZE + input_records_for_one_page_vec[top_run_idx]*entry_size;
            memcpy(tmp_offset1, tmp_offset2, entry_size);
            output_records_counter_for_one_page++;
            if(output_records_counter_for_one_page == entries_per_page){
                counter += entries_per_page;
                seq_write_cnt++;
                write_and_clear_one_page(output_fd, output_buffer);
                output_records_counter_for_one_page = 0U;
            }
            input_records_for_one_page_vec[top_run_idx]++;
            if(input_records_for_one_page_vec[top_run_idx] == entries_per_page){
                input_records_for_one_page_vec[top_run_idx] = 0;
                tmp_offset2 = input_buffer + top_run_idx*DB_PAGE_SIZE;
                memset(tmp_offset2, 0, DB_PAGE_SIZE);
                if(fd_vec[top_run_idx] != -1){
                    read_bytes = read_one_page(fd_vec[top_run_idx], tmp_offset2);
                    if(read_bytes <= 0){
                        memset(tmp_offset2, 0, DB_PAGE_SIZE);
                        close(fd_vec[top_run_idx]);
                        fd_vec[top_run_idx] = -1;
                    }
                    input_read_pages_vec[top_run_idx]++;
                } 
            }else{
                tmp_offset2 += entry_size;
            }

            if(input_records_for_one_page_vec[top_run_idx] + input_read_pages_vec[top_run_idx]*entries_per_page < last_num_entries_per_merging_run[top_run_idx]){
                ByteArray2String(std::string(tmp_offset2, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);    
                pq.emplace(tmp_str, top_run_idx);
            }
        }

        if(output_records_counter_for_one_page > 0){
            counter += output_records_counter_for_one_page;
            seq_write_cnt++;
            write_and_clear_one_page(output_fd, output_buffer);
            output_records_counter_for_one_page = 0U;
        }
        fsync(output_fd);
	ftruncate(output_fd, (off_t)(ceil(counter*1.0/entries_per_page)*DB_PAGE_SIZE));
        close(output_fd);

        run_idx = tmp_run_idx;
        num_entries_per_run.push_back(counter);
        counter = 0;
        new_run_num++; 
    }
    /*
    for(uint32_t i = 0; i < num_runs; i++){
    remove(std::string(prefix_input_filename + std::to_string(i)).c_str());
    }*/
    return;
}

template <typename T> void Emulator::merge_join(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint8_t left_pass_no, uint8_t right_pass_no, std::vector<uint32_t> left_num_entries_per_run, std::vector<uint32_t> right_num_entries_per_run){
    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
    read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
   
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    if(!params_.no_direct_io){
        write_flags |= O_DIRECT;
    }
    if(!params_.no_sync_io){
        write_flags |= O_SYNC;
    }
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    uint32_t left_num_runs = left_num_entries_per_run.size();
    uint32_t right_num_runs = right_num_entries_per_run.size();

    std::vector<int> left_fd_vec = std::vector<int> (left_num_runs, -1);
    std::vector<uint32_t> left_input_records_for_one_page_vec = std::vector<uint32_t> (left_num_runs, 0);
    std::vector<uint32_t> left_pages_read_vec = std::vector<uint32_t> (left_num_runs, 0);
    uint32_t left_entries_per_page = DB_PAGE_SIZE/left_entry_size;
    uint32_t left_value_size = left_entry_size - params_.join_key_size;
    char* left_input_buffer;
    posix_memalign((void**)&left_input_buffer,DB_PAGE_SIZE,left_num_runs*DB_PAGE_SIZE);
    memset(left_input_buffer, 0, left_num_runs*DB_PAGE_SIZE);
    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T > left_pq;

    std::vector<int> right_fd_vec = std::vector<int> (right_num_runs, -1);
    std::vector<uint32_t> right_input_records_for_one_page_vec = std::vector<uint32_t> (right_num_runs, 0);
    std::vector<uint32_t> right_pages_read_vec = std::vector<uint32_t> (right_num_runs, 0);
    uint32_t right_entries_per_page = DB_PAGE_SIZE/right_entry_size;
    char* right_input_buffer;
    posix_memalign((void**)&right_input_buffer,DB_PAGE_SIZE,right_num_runs*DB_PAGE_SIZE);
    memset(right_input_buffer, 0, right_num_runs*DB_PAGE_SIZE);
    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T > right_pq;



    // load the first pages
    std::string tmp_str;
    std::string tmp_str2;
    char* tmp_offset;
    uint32_t read_bytes = 0;
    std::string prefix_input_filename = left_file_prefix + "p" + std::to_string(left_pass_no) + "-run";
    for(uint32_t i = 0; i < left_num_runs; i++){
        tmp_str = prefix_input_filename + std::to_string(i);
        left_fd_vec[i] = open(tmp_str.c_str(), read_flags, read_mode);
        tmp_offset = left_input_buffer + i*DB_PAGE_SIZE;
        read_bytes = read_one_page(left_fd_vec[i], tmp_offset);
        if(read_bytes <= 0){
            left_fd_vec[i] = -1;
             memset(tmp_offset, 0, DB_PAGE_SIZE);
        }else{
            ByteArray2String(std::string(tmp_offset, params_.join_key_size), tmp_str2, params_.join_key_type, params_.join_key_size);
            left_pq.emplace(tmp_str2, i); 
        }
    }

    prefix_input_filename = right_file_prefix + "p" + std::to_string(right_pass_no) + "-run";
    for(uint32_t i = 0; i < right_num_runs; i++){
        tmp_str = prefix_input_filename + std::to_string(i);
        right_fd_vec[i] = open(tmp_str.c_str(), read_flags, read_mode);
        tmp_offset = right_input_buffer + i*DB_PAGE_SIZE;
        read_bytes = read_one_page(right_fd_vec[i], tmp_offset);
        if(read_bytes <= 0){
            right_fd_vec[i] = -1;
            memset(tmp_offset, 0, DB_PAGE_SIZE);
        }else{
            ByteArray2String(std::string(tmp_offset, params_.join_key_size), tmp_str2, params_.join_key_type, params_.join_key_size);
            right_pq.emplace(tmp_str2, i); 
        }
    }

    // join
    char* tmp_offset2;
    uint32_t left_tmp_run_idx;
    uint32_t right_tmp_run_idx;
    
    if(!left_pq.empty()){
    tmp_str = left_pq.top().first;
    left_tmp_run_idx = left_pq.top().second;
    tmp_offset = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE + left_input_records_for_one_page_vec[left_tmp_run_idx]*left_entry_size;
    }
    int matched_result;
    while(!left_pq.empty() && !right_pq.empty()){ // assuming pk-fk join (the left relation is the one with the primary key)
    
        matched_result = tmp_str.compare(right_pq.top().first);

        right_tmp_run_idx = right_pq.top().second; 
        if(matched_result == 0){
            add_one_record_into_join_result_buffer(right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE + right_input_records_for_one_page_vec[right_tmp_run_idx]*right_entry_size, right_entry_size, tmp_offset, left_entry_size);
        }

        if(matched_result == 0 || (params_.SMJ_greater_flag && matched_result > 0) || (!params_.SMJ_greater_flag && matched_result < 0) ){
            right_pq.pop();
            right_input_records_for_one_page_vec[right_tmp_run_idx]++;
            if(right_input_records_for_one_page_vec[right_tmp_run_idx] == right_entries_per_page){
                right_input_records_for_one_page_vec[right_tmp_run_idx] = 0;
                tmp_offset2 = right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE;
                memset(tmp_offset2, 0, DB_PAGE_SIZE);
                if(right_fd_vec[right_tmp_run_idx] != -1){
                    read_bytes = read_one_page(right_fd_vec[right_tmp_run_idx], tmp_offset2);
                        if(read_bytes <= 0){
                        memset(tmp_offset2, 0, DB_PAGE_SIZE);
                        close(right_fd_vec[right_tmp_run_idx]);
                        right_fd_vec[right_tmp_run_idx] = -1;
                    }
                    right_pages_read_vec[right_tmp_run_idx]++;
                }
    
            }else{
                tmp_offset2 = right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE + right_input_records_for_one_page_vec[right_tmp_run_idx]*right_entry_size;
            }

            if(right_pages_read_vec[right_tmp_run_idx]*right_entries_per_page + right_input_records_for_one_page_vec[right_tmp_run_idx] < right_num_entries_per_run[right_tmp_run_idx]){
                ByteArray2String(std::string(tmp_offset2, params_.join_key_size),  tmp_str2, params_.join_key_type, params_.join_key_size);
                right_pq.emplace(tmp_str2, right_tmp_run_idx);    
            }
        }else{
            left_pq.pop();
            left_input_records_for_one_page_vec[left_tmp_run_idx]++;
            if(left_input_records_for_one_page_vec[left_tmp_run_idx] == left_entries_per_page){
                    left_input_records_for_one_page_vec[left_tmp_run_idx] = 0;
            tmp_offset2 = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE;
            memset(tmp_offset2, 0, DB_PAGE_SIZE);
            if(left_fd_vec[left_tmp_run_idx] != -1){
                read_bytes = read_one_page(left_fd_vec[left_tmp_run_idx], tmp_offset2);
                if(read_bytes <= 0){
                    memset(tmp_offset2, 0, DB_PAGE_SIZE);
                    close(left_fd_vec[left_tmp_run_idx]);
                    left_fd_vec[left_tmp_run_idx] = -1;
                }
                left_pages_read_vec[left_tmp_run_idx]++;
            }
    
            }else{
                tmp_offset2 = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE + left_input_records_for_one_page_vec[left_tmp_run_idx]*left_entry_size;
            }

            if(left_pages_read_vec[left_tmp_run_idx]*left_entries_per_page + left_input_records_for_one_page_vec[left_tmp_run_idx] < left_num_entries_per_run[left_tmp_run_idx]){
                ByteArray2String(std::string(tmp_offset2, params_.join_key_size), tmp_str2, params_.join_key_type, params_.join_key_size);
                left_pq.emplace(tmp_str2, left_tmp_run_idx);    
            }

            if(!left_pq.empty()){
                tmp_str = left_pq.top().first;
                left_tmp_run_idx = left_pq.top().second;
                tmp_offset = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE + left_input_records_for_one_page_vec[left_tmp_run_idx]*left_entry_size;
            }
        }
        
    }

    /*
    prefix_input_filename = left_file_prefix + "-p" + std::to_string(left_pass_no) + "-run";
    for(uint32_t i = 0; i < left_num_runs; i++){
    remove(std::string(prefix_input_filename + std::to_string(i)).c_str());
    }

    prefix_input_filename = right_file_prefix + "-p" + std::to_string(right_pass_no) + "-run";
    for(uint32_t i = 0; i < right_num_runs; i++){
    remove(std::string(prefix_input_filename + std::to_string(i)).c_str());
    }*/
    return;

}

void Emulator::get_emulated_cost_SMJ(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_SMJ(params_.workload_rel_R_path, params_.workload_rel_S_path, "part_rel_R/", "part_rel_S/", params_.left_table_size, params_.right_table_size, 0);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}

void Emulator::get_emulated_cost_SMJ(std::string left_file_name, std::string right_file_name, std::string left_prefix, std::string right_prefix, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){
    
    uint8_t left_num_pass = 0;
    uint8_t right_num_pass = 0;
    uint32_t num_R_runs, num_S_runs;

    std::vector<uint32_t> num_R_entries_per_run;
    std::vector<uint32_t> num_S_entries_per_run;

    double left_selection_ratio = params_.left_selection_ratio;
    double right_selection_ratio = params_.right_selection_ratio;
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = (params_.tpch_q12_flag && depth == 0) ? 1 : 0;
    if(depth != 0){
     left_selection_ratio = 1.0;
     right_selection_ratio = 1.0;
    }
    if(params_.SMJ_greater_flag){
        internal_sort<std::greater<std::pair<std::string, std::string>>>(left_file_name, left_prefix, params_.left_E_size, left_num_entries, left_selection_ratio, &left_selection_seed, R_filter_condition, num_R_entries_per_run);
        num_R_runs = num_R_entries_per_run.size();
        //std::cout << "num_R_runs : " << num_R_runs << std::endl;
        internal_sort<std::greater<std::pair<std::string, std::string>>>(right_file_name, right_prefix, params_.right_E_size, right_num_entries, right_selection_ratio, &right_selection_seed, S_filter_condition, num_S_entries_per_run);
        num_S_runs = num_S_entries_per_run.size();
        //std::cout << "num_S_runs : " << num_S_runs << std::endl;
    
        while(num_R_runs + num_S_runs > params_.B - 1){
            if(num_R_runs < num_S_runs){
                if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1){
                            merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
                    num_R_runs = num_R_entries_per_run.size();
                    left_num_pass++;
                }else if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1 || num_R_runs == 1){
                            merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
                    num_S_runs = num_S_entries_per_run.size();
                    right_num_pass++;
                }else{
                            merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
                    num_R_runs = num_R_entries_per_run.size();
                            merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
                    num_S_runs = num_S_entries_per_run.size();
                    left_num_pass++;
                    right_num_pass++;
                }
            }else{
                    if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1){
                        merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
                num_S_runs = num_S_entries_per_run.size();
                right_num_pass++;
            }else if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1 || num_S_runs == 1){
                        merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
                num_R_runs = num_R_entries_per_run.size();
                left_num_pass++;
            }else{
                        merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
                num_R_runs = num_R_entries_per_run.size();
                //cout << num_R_runs << endl;
                        merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
                num_S_runs = num_S_entries_per_run.size();
                left_num_pass++;
                right_num_pass++;
            }
        }

           if (params_.debug) {       
        std::cout << "pass no: " << (int)left_num_pass << " done (#runs for R: " << num_R_runs << "), pass no: " << (int)right_num_pass << "  (#runs for S: " << num_S_runs << ")" << std::endl;
        }
        }
        merge_join<std::greater<std::pair<std::string, uint32_t>>>(left_prefix, right_prefix, params_.left_E_size, params_.right_E_size, left_num_pass, right_num_pass, num_R_entries_per_run, num_S_entries_per_run); 
    }else{
    internal_sort<std::less<std::pair<std::string, std::string>>>(left_file_name, left_prefix, params_.left_E_size, left_num_entries, right_selection_ratio, &right_selection_seed, R_filter_condition, num_R_entries_per_run);
    num_R_runs = num_R_entries_per_run.size();
    //std::cout << "num_R_runs : " << num_R_runs << std::endl;
        internal_sort<std::less<std::pair<std::string, std::string>>>(right_file_name, right_prefix, params_.right_E_size, right_num_entries, left_selection_ratio, &left_selection_seed, S_filter_condition, num_S_entries_per_run);
        num_S_runs = num_S_entries_per_run.size();
    //std::cout << "num_S_runs : " << num_S_runs << std::endl;
    
        while(num_R_runs + num_S_runs > params_.B - 1){
       if(num_R_runs < num_S_runs){
        if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1){
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
            num_R_runs = num_R_entries_per_run.size();
            left_num_pass++;
        }else if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1 || num_R_runs == 1){
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
            num_S_runs = num_S_entries_per_run.size();
            right_num_pass++;
        }else{
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
            num_R_runs = num_R_entries_per_run.size();
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
            num_S_runs = num_S_entries_per_run.size();
            left_num_pass++;
            right_num_pass++;
        }
        }else{
                if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1){
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
            num_S_runs = num_S_entries_per_run.size();
            right_num_pass++;
        }else if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1 || num_S_runs == 1){
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
            num_R_runs = num_R_entries_per_run.size();
            left_num_pass++;
        }else{
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, right_prefix, params_.right_E_size, num_S_runs, right_num_pass, num_S_entries_per_run); 
            num_S_runs = num_S_entries_per_run.size();
                    merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, left_prefix, params_.left_E_size, num_R_runs, left_num_pass, num_R_entries_per_run);
            num_R_runs = num_R_entries_per_run.size();
            left_num_pass++;
            right_num_pass++;
        }
        }
        if(params_.debug){    
        std::cout << "pass no: " << (int)left_num_pass << " done (#runs for R: " << num_R_runs << "), pass no: " << (int)right_num_pass << "  (#runs for S: " << num_S_runs << ")" << std::endl;
        }
        }
        merge_join<std::less<std::pair<std::string, uint32_t>>>(left_prefix, right_prefix, params_.left_E_size, params_.right_E_size, left_num_pass, right_num_pass, num_R_entries_per_run, num_S_entries_per_run); 
    }
    

}

void Emulator::partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::unordered_set<std::string> & in_memory_keys,
std::unordered_map<std::string, std::string> & key2Rvalue, uint32_t num_pre_partitions, uint32_t num_random_in_mem_partitions, std::string file_name, uint32_t entry_size, uint32_t num_entries, uint32_t divider,
double selection_ratio, uint64_t* selection_seed, std::string prefix, uint32_t depth, uint32_t filter_condition, bool build_in_mem_partition_flag){
    std::mt19937 selection_generator (*selection_seed); 

    bool prepartitioned = num_pre_partitions > 0 || in_memory_keys.size() > 0;
    //if (divider + num_pre_partitions < params_.num_partitions) divider = 0;
    build_in_mem_partition_flag = build_in_mem_partition_flag && params_.hybrid;
    if (build_in_mem_partition_flag) key2Rvalue.clear();
    
    bool probe_in_mem_partition_flag = !build_in_mem_partition_flag && params_.hybrid && key2Rvalue.size() != 0;
    
    
    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    char* buffer;
    posix_memalign((void**)&buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    memset(buffer, 0, DB_PAGE_SIZE);
    char* output_buffer;
    posix_memalign((void**)&output_buffer,DB_PAGE_SIZE,params_.num_partitions*DB_PAGE_SIZE);
    memset(output_buffer, 0, params_.num_partitions*DB_PAGE_SIZE);
    char* tmp_addr = NULL;
    char* tmp_output_buffer = NULL;
    std::vector<uint32_t>* offsets = new std::vector<uint32_t> (params_.num_partitions, 0);
    std::vector<std::vector<std::string> > in_memory_entries;
    std::vector<bool> random_in_mem_partitions_spilled_out_flags;
    int random_in_memory_partition_idx_to_be_evicted = 0;
    uint32_t num_random_in_memory_entries = 0;
    uint32_t upper_bound_of_num_random_in_memory_entries = 0;
    uint32_t num_no_filtered_random_entries = num_entries - in_memory_keys.size() - partitioned_keys.size();
    if (depth >= 1 && selection_ratio < 1.0) num_no_filtered_random_entries *= selection_ratio;
    //uint64_t estimated_partition_size = (uint64_t)ceil((num_entries*selection_ratio/(log(params_.num_partitions - (build_in_mem_partition_flag ? 1: 0))))/floor(DB_PAGE_SIZE*1.0/entry_size))*DB_PAGE_SIZE;
    uint64_t estimated_partition_size = (uint64_t)ceil((2*num_no_filtered_random_entries/((params_.num_partitions - num_random_in_mem_partitions)))/floor(DB_PAGE_SIZE*1.0/entry_size))*DB_PAGE_SIZE;
    if (divider > 0) {
        estimated_partition_size = (uint64_t)ceil((2*num_no_filtered_random_entries/(divider))/floor(DB_PAGE_SIZE*1.0/entry_size))*DB_PAGE_SIZE;
    }

    if (build_in_mem_partition_flag && num_random_in_mem_partitions > 0) {
        in_memory_entries.resize(num_random_in_mem_partitions);
        random_in_mem_partitions_spilled_out_flags.resize(num_random_in_mem_partitions, false);
        random_in_memory_partition_idx_to_be_evicted = num_random_in_mem_partitions - 1;
	uint32_t uu = params_.B - 2 - params_.num_partitions
					- ceil(in_memory_keys.size()*params_.left_E_size*FUDGE_FACTOR/DB_PAGE_SIZE)
					- get_hash_map_size(in_memory_keys.size(), params_.join_key_size, 0)
					- get_hash_map_size(partitioned_keys.size(), params_.join_key_size, 0);
	upper_bound_of_num_random_in_memory_entries = (uint32_t)floor(floor((params_.B - 2 - (params_.num_partitions - num_random_in_mem_partitions)
					- ceil(in_memory_keys.size()*params_.left_E_size*FUDGE_FACTOR/DB_PAGE_SIZE)
					- get_hash_map_size(in_memory_keys.size(), params_.join_key_size, 0)
					- get_hash_map_size(partitioned_keys.size(), params_.join_key_size, 0))*DB_PAGE_SIZE/FUDGE_FACTOR)/params_.left_E_size);
        // add one temporary output buffer to spill out partitions, this is only used when partitioning
        // relation R, and thus can replace the join output page that is not used in this phase
        posix_memalign((void**)&tmp_output_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    }
    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
        read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH; 
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    if(!params_.no_direct_io){
        write_flags |= O_DIRECT;
    }
    if(!params_.no_sync_io){
        write_flags |= O_SYNC;
    }
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    uint64_t hash_value; 
    HashType tmp_ht = params_.ht;
    tmp_ht = static_cast<HashType> ((tmp_ht + depth)%6U);
    uint32_t subpartition_idx = 0;
    ssize_t read_bytes = 0;
    int fd;
    uint32_t read_entries = 0;
    fd = open(file_name.c_str(), read_flags, read_mode); 

    std::vector<int>* fd_vec = new std::vector<int> ( params_.num_partitions, -1);

    std::string tmp_str = "";
    uint32_t i, j, l;

    auto spill_out_partition = [&] (uint32_t subpart_idx_to_be_evicted, uint32_t random_in_memory_part_idx_to_be_evicted) {
        if (fd_vec->at(subpart_idx_to_be_evicted) == -1) {
            // the associated file is not being opened yet
            tmp_str = prefix + "-part-" + std::to_string(depth) + "-" + std::to_string(subpart_idx_to_be_evicted);
            fd_vec->at(subpart_idx_to_be_evicted) = open(tmp_str.c_str(), write_flags, write_mode);
	    posix_fallocate(fd_vec->at(subpart_idx_to_be_evicted), 0, estimated_partition_size);
            std::cout << "One partition with " << in_memory_entries[random_in_memory_part_idx_to_be_evicted].size() << " entries is being spilled to disk" << std::endl;
        }
        
        j = 0;
        memset(tmp_output_buffer, 0, DB_PAGE_SIZE);
        // spill out the partition
        while(j < in_memory_entries[random_in_memory_part_idx_to_be_evicted].size()) {
            l = 0;
            while (l < entries_per_page && j < in_memory_entries[random_in_memory_part_idx_to_be_evicted].size()) {
                memcpy(tmp_output_buffer + l*entry_size, in_memory_entries[random_in_memory_part_idx_to_be_evicted][j].c_str(), entry_size);
                l++;
                j++;
            }
            write_and_clear_one_page(fd_vec->at(subpart_idx_to_be_evicted), tmp_output_buffer);
        }
        random_in_mem_partitions_spilled_out_flags[random_in_memory_part_idx_to_be_evicted] = true;
    };
    while(true){
        read_bytes = read_one_page(fd, buffer);
        if(read_bytes <= 0) break;

        for(i = 0; i < entries_per_page && read_entries < num_entries; i++){    
            ByteArray2String(std::string(buffer + i*entry_size, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);
            read_entries++;
            if (depth == 0 && filter_condition > 0) {
                if(!is_qualified_for_condition(std::string(buffer + i*entry_size, entry_size), filter_condition)) {
                    continue;
                }
            }else if(depth == 0 && selection_ratio < 1.0 && selection_dist(selection_generator) >= selection_ratio){
                continue;
            }

            if(!prepartitioned){
                if (probe_in_mem_partition_flag && key2Rvalue.find(tmp_str) != key2Rvalue.end()) {
                    add_one_record_into_join_result_buffer(buffer + i*entry_size, params_.right_E_size, key2Rvalue[tmp_str].c_str(), params_.left_E_size);
                    continue;
                }
                hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
                subpartition_idx = hash_value%params_.num_partitions;
                if(divider != 0){
                    if (divider< params_.num_partitions) {
                        if (params_.num_partitions > subpartition_idx + num_random_in_mem_partitions) {
                            subpartition_idx = hash_value%(divider);
                        } 
                    } else {
                        subpartition_idx = (hash_value%(divider))%(params_.num_partitions);
                    }        
                }
            }else{
            
                if (build_in_mem_partition_flag && in_memory_keys.find(tmp_str) != in_memory_keys.end()) {
                    key2Rvalue[tmp_str] = std::string(buffer + i*entry_size, entry_size);
                    continue;
                } else if(probe_in_mem_partition_flag && key2Rvalue.find(tmp_str) != key2Rvalue.end()) {
                    add_one_record_into_join_result_buffer(buffer + i*entry_size, params_.right_E_size, key2Rvalue[tmp_str].c_str(), params_.left_E_size);
                    continue;
                } else if(partitioned_keys.find(tmp_str) == partitioned_keys.end()){
                    hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
                    if (params_.num_partitions == num_random_in_mem_partitions) {
                        subpartition_idx = hash_value%num_random_in_mem_partitions;
                    } else if(divider != 0){
                        subpartition_idx = hash_value%(params_.num_partitions - num_pre_partitions);
                        if (divider + num_pre_partitions < params_.num_partitions) {
                            if (params_.num_partitions > subpartition_idx + num_random_in_mem_partitions) {
                                subpartition_idx = hash_value%(divider);
                            } 
                        } else {
                            subpartition_idx = (hash_value%(divider))%(params_.num_partitions - num_pre_partitions);
                        }
                        subpartition_idx += num_pre_partitions;
                    }else{
                        // when params_.num_partitions == num_pre_partitions, all keys that have matching keys should have
                        // been prepartitioned, only keys that do not have matching will not appear in the pre-partitioned
                        // list. Therefore we can ignore keys that do not have any matchings
                        if(params_.num_partitions == num_pre_partitions) continue; 
                        
                        subpartition_idx = hash_value%(params_.num_partitions - num_pre_partitions);
                        subpartition_idx += num_pre_partitions;
                    }
                }else if(num_pre_partitions != 1){
                    subpartition_idx = partitioned_keys.at(tmp_str);
                }else{
                    subpartition_idx = 0;
                }
            }

            memcpy(output_buffer + subpartition_idx*DB_PAGE_SIZE+offsets->at(subpartition_idx), buffer + i*entry_size, entry_size);

            offsets->at(subpartition_idx) += entry_size;
            if(DB_PAGE_SIZE < offsets->at(subpartition_idx) + entry_size){
                if (build_in_mem_partition_flag && params_.num_partitions <= subpartition_idx + num_random_in_mem_partitions) {
                    // the index for random in-memory partition starts from the end of the partition list due to the rounded-hash,
                    // partitions with larger subpartition_idx could tend to be smaller than ones with smaller subpartition_idx, thus
                    // less possible to trigger spilling
                    if (!random_in_mem_partitions_spilled_out_flags[params_.num_partitions - subpartition_idx - 1] && upper_bound_of_num_random_in_memory_entries > 0) {
                        // when the partition is not spilled out, put all the entries in the output page into the in-memory list
                        tmp_addr = output_buffer + subpartition_idx*DB_PAGE_SIZE;
                        for (j = 0; j < entries_per_page; j++) {
                            in_memory_entries[params_.num_partitions - subpartition_idx - 1].emplace_back(tmp_addr + j*entry_size, entry_size);
                        }
                        memset(tmp_addr, 0, DB_PAGE_SIZE); 
                        num_random_in_memory_entries += entries_per_page;

                        while (num_random_in_memory_entries > upper_bound_of_num_random_in_memory_entries) {
                            // when the in-memory partitions are too large to fit, spill out a partition with larger size)
			    int tmp_in_memory_partition_idx_to_be_evicted = (int)in_memory_entries.size() - 1;
		            random_in_memory_partition_idx_to_be_evicted = 0;
		            size_t max_partition_size = in_memory_entries[tmp_in_memory_partition_idx_to_be_evicted].size();
			    for (;tmp_in_memory_partition_idx_to_be_evicted >= 0; tmp_in_memory_partition_idx_to_be_evicted--) {
				if (in_memory_entries[tmp_in_memory_partition_idx_to_be_evicted].size() == 0) {
				    continue;
				} else if (in_memory_entries[tmp_in_memory_partition_idx_to_be_evicted].size() > max_partition_size) {
				    max_partition_size = in_memory_entries[tmp_in_memory_partition_idx_to_be_evicted].size();
                                    random_in_memory_partition_idx_to_be_evicted = tmp_in_memory_partition_idx_to_be_evicted;
				}
			    }

                            spill_out_partition(params_.num_partitions - 1 - random_in_memory_partition_idx_to_be_evicted, (uint32_t) random_in_memory_partition_idx_to_be_evicted);
                            num_random_in_memory_entries -= in_memory_entries[random_in_memory_partition_idx_to_be_evicted].size();
                            in_memory_entries[random_in_memory_partition_idx_to_be_evicted].clear();
                        }
                    } else {
			// The partition has been spilled out with file opened, no need to check if the file is open again
			write_and_clear_one_page(fd_vec->at(subpartition_idx), output_buffer + subpartition_idx*DB_PAGE_SIZE);
		    }
                } else {
                    if(fd_vec->at(subpartition_idx) == -1){
                        tmp_str = prefix + "-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx);
                        fd_vec->at(subpartition_idx) = open(tmp_str.c_str(), write_flags, write_mode);
			posix_fallocate(fd_vec->at(subpartition_idx), 0, estimated_partition_size);
                    }
                    write_and_clear_one_page(fd_vec->at(subpartition_idx), output_buffer + subpartition_idx*DB_PAGE_SIZE);
                }
                
                offsets->at(subpartition_idx) = 0;
            }
            counter[subpartition_idx]++;
        }
    }
    
    // check if we can still add some entries from the output buffer to the in-memory list, otherwise we have to spill out some partitions
    if (build_in_mem_partition_flag && num_random_in_memory_entries < upper_bound_of_num_random_in_memory_entries) {
        for (i = 0; i < num_random_in_mem_partitions; i++) {
            subpartition_idx = params_.num_partitions - 1 - i;
            
            if (random_in_mem_partitions_spilled_out_flags[i] || offsets->at(subpartition_idx) == 0) continue;
            uint32_t num_entries_in_output_buffer = offsets->at(subpartition_idx)/entry_size;
            if (num_random_in_memory_entries + num_entries_in_output_buffer < upper_bound_of_num_random_in_memory_entries) {
                tmp_addr = output_buffer + subpartition_idx*DB_PAGE_SIZE;
                j = 0;
                while (j < num_entries_in_output_buffer) {
                    in_memory_entries[i].emplace_back(tmp_addr + j*entry_size, entry_size);
                    j++;
                }
                num_random_in_memory_entries += num_entries_in_output_buffer;
                offsets->at(subpartition_idx) = 0;
            } else {
                spill_out_partition(subpartition_idx, i);
            }
        }

        // populate key2RValue
        for (i = 0; i < num_random_in_mem_partitions; i++) {
            if (random_in_mem_partitions_spilled_out_flags[i] || in_memory_entries[i].size() == 0) continue;
            for (const std::string s: in_memory_entries[i]) {    
                    ByteArray2String(s.substr(0, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);
                    key2Rvalue[tmp_str] = s;
            }
            in_memory_entries[i].clear();
        }
        in_memory_entries.clear();
        random_in_mem_partitions_spilled_out_flags.clear();
    }

    // flush all the output pages
    for(auto i = 0; i < params_.num_partitions; i++){
        if(offsets->at(i) != 0){
            if(fd_vec->at(i) == -1){
                tmp_str = prefix + "-part-" + std::to_string(depth) + "-" + std::to_string(i);
                fd_vec->at(i) = open(tmp_str.c_str(), write_flags, write_mode);
            }
            write_and_clear_one_page(fd_vec->at(i), output_buffer + i*DB_PAGE_SIZE);
        }
	
	if(fd_vec->at(i) != -1){
            fsync(fd_vec->at(i));
	    
	    if (counter[i] != 0) {
		ftruncate(fd_vec->at(i), (off_t)(ceil(counter[i]*1.0/entries_per_page)*DB_PAGE_SIZE));
	    }
            close(fd_vec->at(i));
        }
        // fd for in-memory partitions are never opened
    }
    offsets->clear();
    delete offsets;
    fd_vec->clear();
    delete fd_vec;
    close(fd);

}

void Emulator::get_emulated_cost_GHJ(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_GHJ(params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();

}


void Emulator::get_emulated_cost_GHJ(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){
    double left_selection_ratio = 1.0;
    double right_selection_ratio = 1.0;
    if (depth == 0) {
    left_selection_ratio = params_.left_selection_ratio;
    right_selection_ratio = params_.right_selection_ratio;
    }    

    // only set filter condition of S for now in case of tpch Q12 query 
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = params_.tpch_q12_flag ? 1 : 0;


    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    double num_passes_R = ceil(left_num_entries*left_selection_ratio/step_size);
    if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(params_.left_table_size*left_selection_ratio/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*ceil(params_.right_table_size/right_entries_per_page)){
	std::cout << "Automatically change to NBJ since GHJ has higher cost by estimation. " << std::endl;
        probe_start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_NBJ(left_file_name, right_file_name, params_.left_table_size, params_.right_table_size, depth, R_filter_condition, S_filter_condition, true);
        probe_end = std::chrono::high_resolution_clock::now();
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
        return;
    }
    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);

    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();


    std::unordered_map<std::string, std::string> key2Rvalue;

    if(rounded_hash){

        if(depth == 0) std::cout << "Num passes R: " << num_passes_R << "\t" << params_.num_partitions << std::endl;
        num_passes_R = ceil(left_num_entries*left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent));
        if(num_passes_R < (2 + params_.randwrite_seqread_ratio)*params_.num_partitions && num_passes_R > (2 + params_.seqwrite_seqread_ratio)*params_.num_partitions){
            num_passes_R = ceil(left_num_entries*left_selection_ratio/((2+params_.seqwrite_seqread_ratio)*(step_size*params_.hashtable_fulfilling_percent)));
        }

    if(left_num_entries*left_selection_ratio/params_.num_partitions/step_size > 2 + params_.seqwrite_seqread_ratio){
        num_passes_R = 0;
    }

    if(num_passes_R < (2 +params_.seqwrite_seqread_ratio)*params_.num_partitions && num_passes_R != 0 && ceil(left_num_entries*left_selection_ratio/params_.num_partitions/step_size/params_.hashtable_fulfilling_percent) - ceil(left_num_entries*left_selection_ratio/params_.num_partitions/step_size) >= 1){
       //  do not use round hash if the filling percentage has go beyond the threshold
        num_passes_R = 0;
    }
        partition_file(counter_R, {}, {}, key2Rvalue, 0, 0, left_file_name, params_.left_E_size, left_num_entries, num_passes_R, left_selection_ratio, &left_selection_seed, "part_rel_R/R", depth, R_filter_condition); 
        partition_file(counter_S, {}, {}, key2Rvalue, 0, 0, right_file_name, params_.right_E_size, right_num_entries, num_passes_R, right_selection_ratio, &right_selection_seed, "part_rel_S/S", depth, S_filter_condition); 
    }else{
        partition_file(counter_R, {}, {}, key2Rvalue, 0, 0, left_file_name, params_.left_E_size, left_num_entries, 0, left_selection_ratio, &left_selection_seed, "part_rel_R/R", depth, R_filter_condition); 
        partition_file(counter_S, {}, {}, key2Rvalue, 0, 0, right_file_name, params_.right_E_size, right_num_entries, 0, right_selection_ratio, &right_selection_seed, "part_rel_S/S", depth, S_filter_condition); 
    }
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);

    uint32_t x = 0;
    bool SMJ_flag = false;
    
    //probing
    for(auto i = 0; i < params_.num_partitions; i++){

    if(counter_R[i] == 0 || counter_S[i] == 0){        
        // if(counter_R[i] != 0) 
        //     remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        // if(counter_S[i] != 0) 
        //     remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
        
        continue;
    }
    num_passes_R = ceil(counter_R[i]*1.0/step_size);
    if(params_.debug && depth == 0){
        std::cout << "counter_R : " << counter_R[i] << " --- counter_S : " << counter_S[i] << std::endl;
    }
    
    SMJ_flag = (!params_.no_smj_partition_wise_join) && ((ceil(counter_S[i]*1.0/(right_entries_per_page*2*(params_.B - 1))) + ceil(counter_R[i]*1.0/(left_entries_per_page*2*(params_.B - 1)))) < (params_.B - 1));
    if((num_passes_R <= 2 + params_.seqwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.seqwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page))|| (num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*ceil(counter_S[i]/right_entries_per_page)) && !SMJ_flag){
        if(params_.debug && depth == 0) std::cout << "NBJ" << std::endl;
    //if(num_passes_R <= 1){
        probe_start = std::chrono::high_resolution_clock::now();
        get_emulated_cost_NBJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1, 0, 0, true);
        probe_end = std::chrono::high_resolution_clock::now();
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
        //remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        //remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
    }else if(SMJ_flag){
        if(params_.debug && depth == 0) std::cout << "SMJ" << std::endl;
       get_emulated_cost_SMJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_R/", "part_rel_S/", counter_R[i], counter_S[i], depth + 1);
    }else{
        if(params_.debug && depth == 0) std::cout << "GHJ" << std::endl;

       get_emulated_cost_GHJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1);
        x+= ceil(counter_R[i]*1.0/left_entries_per_page);
    }
    //remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        //remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
    //break; // testing
    
    }
    if(x > 0) std::cout << " repartitioned cost:" << x << std::endl;

}


void Emulator::get_emulated_cost_DHH(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_DHH(params_.workload_rel_R_path, params_.workload_rel_S_path, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();

}


void Emulator::get_emulated_cost_DHH(std::string left_file_name, std::string right_file_name, uint32_t depth){
    uint32_t num_passes_R = ceil(params_.left_table_size*params_.left_selection_ratio/step_size);
    // if(num_passes_R <= 2 + params_.randwrite_seqread_ratio){
    //     get_emulated_cost_NBJ(left_file_name, right_file_name, params_.left_table_size, params_.right_table_size, true);
    //     return;
    // }

    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;
    std::unordered_set<std::string> in_memory_skewed_keys;

    
    
    if (params_.k > 0) {
        
        uint64_t summed_matches = 0U;
        for(uint32_t i = 0; i < keys.size(); i++){
            if(key_multiplicity[i] == 0) continue;
            summed_matches += key_multiplicity[i];
            key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity[i], i));
        }
        if (summed_matches*1.0/(params_.right_table_size*params_.right_selection_ratio) >= params_.DHH_skew_partition_percent) {
            std::sort(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end(), [&](const std::pair<uint32_t, uint32_t> & pair1, const std::pair<uint32_t, uint32_t> & pair2){
                if (pair1.first > pair2.first) {
                    return true;
                } else if(pair1.first < pair2.first) {
                    return false;
                } else {
                    return pair1.second >= pair2.second;
                }
            });

            for (uint32_t i = 0; i < key_multiplicity_to_be_sorted.size(); i++) {
                in_memory_skewed_keys.insert(keys[key_multiplicity_to_be_sorted[i].second]);
            }
        } else {
            // recalculate the number of partitions because frequency is not high enough for skew optimization
            double R_in_pages = ceil((params_.left_table_size*params_.left_selection_ratio)*params_.left_E_size*1.0/DB_PAGE_SIZE);
            params_.num_partitions = std::max(20U, (uint32_t)ceil((R_in_pages*FUDGE_FACTOR - params_.B)/(params_.B - 1)));

            while (ceil(R_in_pages*FUDGE_FACTOR*1.0/params_.num_partitions) + 1 > params_.B) {
                params_.num_partitions++;
            }
                params_.num_partitions = std::min(params_.B - 1, params_.num_partitions);
            std::cout << "Skew optimization is off due to insufficient summed frequencies. #partitions is re-configured as " << params_.num_partitions << "." << std::endl;
        }
    }

    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);

    std::vector<unsigned char> page_out_bits;
    if(params_.num_partitions%8 == 0){
        page_out_bits = std::vector<unsigned char>(params_.num_partitions >> 3, 0U);
    }else{
        page_out_bits = std::vector<unsigned char>((params_.num_partitions >> 3) +1, 0U);
    }
    uint32_t num_paged_out_partitions = 0U;
    uint32_t in_mem_pages = 0U;
    bool curr_page_out = false;

    HashType partition_ht = params_.ht;
    partition_ht = static_cast<HashType> ((partition_ht + depth)%6U);
    HashType probe_ht = static_cast<HashType> ((partition_ht + 1 + depth)%6U);
    //uint64_t estimated_partition_size = (uint64_t)ceil((params_.left_table_size*params_.left_selection_ratio/(log(params_.num_partitions)))/floor(DB_PAGE_SIZE*1.0/params_.left_E_size))*DB_PAGE_SIZE;
    uint64_t estimated_partition_size = (uint64_t)ceil((2*params_.left_table_size*params_.left_selection_ratio/((params_.num_partitions)))/floor(DB_PAGE_SIZE*1.0/params_.left_E_size))*DB_PAGE_SIZE;

    std::chrono::time_point<std::chrono::high_resolution_clock>  io_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  io_end;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();

    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    char* rest_buffer;
    posix_memalign((void**)&rest_buffer,DB_PAGE_SIZE,(params_.B - 2)*DB_PAGE_SIZE);
    memset(rest_buffer, 0, (params_.B - 2)*DB_PAGE_SIZE);

    std::unordered_map<std::string, std::string> key2Rvalue;
    // we maintain this memory-resident memory here, which means that the hash table may co-exist with the pages. We simplify this implementation but this may not occur in the pratical system since there usually exists a global buffer manager that can recylce pages at any time.
    key2Rvalue.clear(); 

    std::vector<uint32_t> offsets = std::vector<uint32_t> (params_.num_partitions, 0);
    std::vector<std::deque<uint32_t> > partitioned_page_idxes = std::vector<std::deque<uint32_t> > (params_.num_partitions, std::deque<uint32_t>());
    std::vector<int> fd_vec = std::vector<int> ( params_.num_partitions, -1);


    int read_flags = O_RDONLY | O_DIRECT;
    if(params_.no_direct_io){
        read_flags = O_RDONLY;
    }
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    if(!params_.no_direct_io){
        write_flags |= O_DIRECT;
    }
    if(!params_.no_sync_io){
        write_flags |= O_SYNC;
    }
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
    uint64_t hash_value; 
    HashType tmp_ht = params_.ht;
    tmp_ht = static_cast<HashType> ((tmp_ht + depth)%6U);

    uint32_t subpartition_idx = 0;
    ssize_t read_bytes = 0;
    int fd_R, fd_S;
    if(depth != 0){
        fd_R = open(("part_rel_R/" + left_file_name).c_str(), read_flags, read_mode);
        fd_S = open(("part_rel_S/" + right_file_name).c_str(), read_flags, read_mode);
    }else{
        fd_R = open(left_file_name.c_str(), read_flags, read_mode);
        fd_S = open(right_file_name.c_str(), read_flags, read_mode);
    }

    std::deque<uint32_t> avail_page_idxes;
    for(uint32_t i = 0; i < (uint32_t) (params_.num_partitions); i++){
        partitioned_page_idxes[i].push_back(i);
    }
    for(uint32_t i = (uint32_t) (params_.num_partitions); i < (uint32_t) (params_.B - 2); i++){
        avail_page_idxes.push_back(i);
    } 
   
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = params_.tpch_q12_flag ? 1: 0;
    //partition R
    uint32_t read_entries = 0;
    std::mt19937 selection_generator_R (params_.left_selection_seed); 
    std::string tmp_str = "";
    std::string tmp_file_str = "";
    uint32_t page_id = 0U;
    uint32_t merged_partition_id = 0U;
    while(true){
        read_bytes = read_one_page(fd_R, input_buffer);
        if(read_bytes <= 0) break;

        for(auto i = 0; i < left_entries_per_page && read_entries < params_.left_table_size; i++){
            ByteArray2String(std::string(input_buffer + i*params_.left_E_size, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);
            read_entries++;
            if(!params_.tpch_q12_flag && R_filter_condition == 0 && params_.left_selection_ratio < 1.0 && selection_dist(selection_generator_R) >= params_.left_selection_ratio) continue;
            if(params_.tpch_q12_flag && R_filter_condition > 0 && !is_qualified_for_condition(std::string(input_buffer + + i*params_.left_E_size, params_.left_E_size), R_filter_condition)) continue;
            if(params_.k > 0 && in_memory_skewed_keys.find(tmp_str) != in_memory_skewed_keys.end()) {
                key2Rvalue[tmp_str] = std::string(input_buffer + i*params_.left_E_size, params_.left_E_size);
                continue;
            }
            hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
            subpartition_idx = hash_value%(params_.num_partitions);
            page_id = partitioned_page_idxes[subpartition_idx].back();
            memcpy(rest_buffer + page_id*DB_PAGE_SIZE+offsets[subpartition_idx], input_buffer + i*params_.left_E_size, params_.left_E_size);
            offsets[subpartition_idx] += params_.left_E_size;
            if(DB_PAGE_SIZE < offsets[subpartition_idx] + params_.left_E_size){
                curr_page_out = (page_out_bits[subpartition_idx/8] >> (subpartition_idx%8)) & 1U;
                if(!curr_page_out){ // if the partition has not been paged out, we check if we can put the extra page in memory
                    in_mem_pages = params_.B - 2 - num_paged_out_partitions;
                    if(avail_page_idxes.size() == 0 || left_entries_per_page*(in_mem_pages - avail_page_idxes.size()) > step_size){ 
                        // when the in-memory hash table can not be held in the memory or when we do not have available pages
                        uint32_t max_size_of_a_partition = 0U;
                        uint32_t partition_idx_to_be_evicted = 0U;
                        for(auto i = 0; i < params_.num_partitions; i++){
                            if((page_out_bits[i/8] >> (i%8)) & 1U) continue;
                            if(max_size_of_a_partition < partitioned_page_idxes[i].size()){
                                max_size_of_a_partition = partitioned_page_idxes[i].size();
                                partition_idx_to_be_evicted = i;
                            }
                        }
                        num_paged_out_partitions++;
                        if(max_size_of_a_partition > 1){
                            if(fd_vec[partition_idx_to_be_evicted] == -1){
                                tmp_file_str = "part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(partition_idx_to_be_evicted);
                                fd_vec[partition_idx_to_be_evicted] = open(tmp_file_str.c_str(), write_flags, write_mode);
				posix_fallocate(fd_vec[partition_idx_to_be_evicted], 0, estimated_partition_size);
                                //if(fd_vec[subpartition_idx] == -1) printf("Error: %s\n", strerror(errno)); 
                            }
                            uint32_t page_id_to_be_evicted = 0U;
                            for(auto i = 0; i < max_size_of_a_partition - 1; i++){
                                page_id_to_be_evicted = partitioned_page_idxes[partition_idx_to_be_evicted].front();
                                partitioned_page_idxes[partition_idx_to_be_evicted].pop_front();
                                write_and_clear_one_page(fd_vec[partition_idx_to_be_evicted], rest_buffer + page_id_to_be_evicted*DB_PAGE_SIZE);
                                avail_page_idxes.push_back(page_id_to_be_evicted);
                            } 
                            page_out_bits[partition_idx_to_be_evicted/8] = page_out_bits[partition_idx_to_be_evicted/8] | (1 << (partition_idx_to_be_evicted%8)); // set the page-out bit
                            if(partition_idx_to_be_evicted == subpartition_idx) curr_page_out = true;

                        }else{ // flush the current partition (will be executed later)
                            page_out_bits[subpartition_idx/8] = page_out_bits[subpartition_idx/8] | (1 << (subpartition_idx%8)); // set the page-out bit
                            curr_page_out = true;
                                    //counter_R[subpartition_idx] = 0;
                        }
                    }
                }

                // The above process may set curr_page_out to true
                if(curr_page_out){
                    if(fd_vec[subpartition_idx] == -1){
                        tmp_file_str = "part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx);
                        fd_vec[subpartition_idx] = open(tmp_file_str.c_str(), write_flags, write_mode);
			posix_fallocate(fd_vec[subpartition_idx], 0, estimated_partition_size);
                        //if(fd_vec[subpartition_idx] == -1) printf("Error: %s\n", strerror(errno)); 
                    }
                    write_and_clear_one_page(fd_vec[subpartition_idx], rest_buffer + partitioned_page_idxes[subpartition_idx][0]*DB_PAGE_SIZE);
                }else{
                    partitioned_page_idxes[subpartition_idx].push_back(avail_page_idxes.front());
                    avail_page_idxes.pop_front();
                }

                offsets[subpartition_idx] = 0;
            }
            counter_R[subpartition_idx]++;

        }
        memset(input_buffer, 0, DB_PAGE_SIZE);
    } 

    
    char* tmp_buffer;
    std::unordered_map<uint32_t, uint32_t> partitionId2outbufferId;
    uint32_t outbufferId = 0U;
    uint32_t acc_num_entries = 0U;
    for(auto i = 0; i < params_.num_partitions; i++){
        if((page_out_bits[i/8] >> (i%8)) & 1U){
            partitionId2outbufferId[i] = outbufferId;
            outbufferId++;
            if(offsets[i] != 0){
                    if(fd_vec[i] == -1){
                    tmp_file_str = "part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i);
                    fd_vec[i] = open(tmp_file_str.c_str(), write_flags, write_mode);
                }
                write_and_clear_one_page(fd_vec[i], rest_buffer + partitioned_page_idxes[i][0]*DB_PAGE_SIZE);
            }
	    if(fd_vec[i] != -1){
                fsync(fd_vec[i]);
		if (counter_R[i] != 0) {
	            ftruncate(fd_vec[i], (off_t)(ceil(counter_R[i]*1.0/floor(DB_PAGE_SIZE*1.0/params_.left_E_size))*DB_PAGE_SIZE));
		}
                close(fd_vec[i]);
            }
        }else{
            for(auto t_page_idx:partitioned_page_idxes[i]){
                tmp_buffer = rest_buffer + t_page_idx*DB_PAGE_SIZE;
                acc_num_entries = 0U;
                for(auto j = 0; j < left_entries_per_page; j++,acc_num_entries++){
                    if(*(tmp_buffer+j*params_.left_E_size) == '\0' && acc_num_entries >= counter_R[i]) break;
                    
                    ByteArray2String(std::string(tmp_buffer + j*params_.left_E_size, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);
                    key2Rvalue[tmp_str] = std::string(tmp_buffer + j*params_.left_E_size, params_.left_E_size);
                }
            }
        }
        partitioned_page_idxes[i].clear();
    }
    delete[] rest_buffer;
    partitioned_page_idxes.clear(); 
    offsets.clear();
    page_out_bits.clear(); // instead of maintaining the bit vector, we use a hash table [partitionId2outbufferId] to maintain the relation between the disk-resident partition id and the output buffer slot
    fd_vec.clear();
    close(fd_R);
    posix_memalign((void**)&rest_buffer,DB_PAGE_SIZE,num_paged_out_partitions*DB_PAGE_SIZE);
    memset(rest_buffer, 0, num_paged_out_partitions*DB_PAGE_SIZE);
    offsets = std::vector<uint32_t> (num_paged_out_partitions, 0);
    fd_vec = std::vector<int> (num_paged_out_partitions, -1);
    std::cout << "#paged-out partitions: " << num_paged_out_partitions << std::endl; 
    if (key2Rvalue.size() > 0) {
        std::cout << key2Rvalue.size() << " records are cached in the partition phase." << std::endl;
    }
    //partition S 
    //estimated_partition_size = (uint64_t)ceil((params_.right_table_size*params_.right_selection_ratio/(log(params_.num_partitions) + 1))/floor(DB_PAGE_SIZE*1.0/params_.right_E_size))*DB_PAGE_SIZE;
    estimated_partition_size = (uint64_t)ceil((2*params_.right_table_size*params_.right_selection_ratio/((params_.num_partitions) + 1))/floor(DB_PAGE_SIZE*1.0/params_.right_E_size))*DB_PAGE_SIZE;
    read_entries = 0;
    std::mt19937 selection_generator_S (params_.right_selection_seed);
    while(true){
        read_bytes = read_one_page(fd_S, input_buffer);
        if(read_bytes <= 0 || read_entries == params_.right_table_size) break;

        for(auto j = 0; j < right_entries_per_page && read_entries < params_.right_table_size; j++){    
            ByteArray2String(std::string(input_buffer + j*params_.right_E_size, params_.join_key_size), tmp_str, params_.join_key_type, params_.join_key_size);
            read_entries++;
            if(!params_.tpch_q12_flag && S_filter_condition == 0 && params_.right_selection_ratio < 1.0 && selection_dist(selection_generator_S) >= params_.right_selection_ratio) continue;
            if(params_.tpch_q12_flag && S_filter_condition > 0 && !is_qualified_for_condition(std::string(input_buffer + j*params_.right_E_size, params_.right_E_size), S_filter_condition)) continue;
            hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
            subpartition_idx = hash_value%(params_.num_partitions);
            if(key2Rvalue.find(tmp_str) != key2Rvalue.end()){ // if it does not find the entry in paged-out partition set
                add_one_record_into_join_result_buffer(input_buffer + j*params_.right_E_size, params_.right_E_size, key2Rvalue[tmp_str].c_str(), params_.left_E_size);
            }else{ // write to the buffer which is going to be flushed when it is full
                page_id = partitionId2outbufferId[subpartition_idx];

                if(fd_vec[page_id] == -1){
                    tmp_file_str = "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx);
                    fd_vec[page_id] = open(tmp_file_str.c_str(), write_flags, write_mode);
		    posix_fallocate(fd_vec[page_id], 0, estimated_partition_size);
                }
                memcpy(rest_buffer + page_id*DB_PAGE_SIZE + offsets[page_id], input_buffer + j*params_.right_E_size, params_.right_E_size);
                offsets[page_id] += params_.right_E_size;

                if(DB_PAGE_SIZE < offsets[page_id] + params_.right_E_size){
                     write_and_clear_one_page(fd_vec[page_id], rest_buffer + page_id*DB_PAGE_SIZE);
                     offsets[page_id] = 0;
                }
                counter_S[subpartition_idx]++;
            }
        }
    }
    close(fd_S);

    for(auto subpartition_idx_iter = partitionId2outbufferId.begin(); subpartition_idx_iter !=  partitionId2outbufferId.end(); subpartition_idx_iter++){
        page_id = subpartition_idx_iter->second;
        if(offsets[page_id] != 0){
            if(fd_vec[page_id] == -1){
                     tmp_file_str = "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx_iter->first);
                     fd_vec[page_id] = open(tmp_file_str.c_str(), write_flags, write_mode);
            }
            write_and_clear_one_page(fd_vec[page_id], rest_buffer + page_id*DB_PAGE_SIZE);
            offsets[page_id] = 0;
        }
	if(fd_vec[page_id] != -1){
            fsync(fd_vec[page_id]);
	    if (counter_S[subpartition_idx_iter->first] != 0) {
		ftruncate(fd_vec[page_id], (off_t)(ceil(counter_S[subpartition_idx_iter->first]*1.0/floor(DB_PAGE_SIZE/params_.right_E_size))*DB_PAGE_SIZE));
	    }
            close(fd_vec[page_id]);
        }
    }

    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    partition_duration += (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    //probing
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t x = 0;
    num_passes_R = 0;
    for(auto subpartition_idx_iter = partitionId2outbufferId.begin(); subpartition_idx_iter !=  partitionId2outbufferId.end(); subpartition_idx_iter++){
        subpartition_idx = subpartition_idx_iter->first;
    
        if(counter_R[subpartition_idx] == 0 || counter_S[subpartition_idx] == 0){
        
            // if(counter_R[subpartition_idx] != 0) 
            //     remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
            // if(counter_S[subpartition_idx] != 0) 
            //     remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());

            continue;
        }

        
        num_passes_R = ceil(counter_R[subpartition_idx]*1.0/step_size);
        if(params_.debug && depth == 0){
            std::cout << "counter_R : " << counter_R[subpartition_idx] << " --- counter_S : " << counter_S[subpartition_idx] << std::endl;
        }
        if(num_passes_R >= 2 + params_.seqwrite_seqread_ratio && counter_S[subpartition_idx]/(right_entries_per_page*2*(params_.B - 1)) + counter_R[subpartition_idx]/(left_entries_per_page*2*(params_.B - 1)) <= params_.B - 1){
           if(params_.debug && depth == 0) std::cout << "SMJ" << std::endl;
           get_emulated_cost_SMJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), "part_rel_R/", "part_rel_S/", counter_R[subpartition_idx], counter_S[subpartition_idx], depth + 1);
        } else if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[subpartition_idx]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[subpartition_idx]/right_entries_per_page)){
        //if(num_passes_R <= 1){
            if(params_.debug && depth == 0) std::cout << "NBJ" << std::endl;
            probe_start = std::chrono::high_resolution_clock::now();
            get_emulated_cost_NBJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), counter_R[subpartition_idx], counter_S[subpartition_idx], depth + 1, 0, 0, true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();    
        }else{
            if(params_.debug && depth == 0) std::cout << "GHJ" << std::endl;
            x+= ceil(counter_R[subpartition_idx]*1.0/left_entries_per_page);
            get_emulated_cost_GHJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(subpartition_idx), counter_R[subpartition_idx], counter_S[subpartition_idx], depth + 1);

        }
    //remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
    //remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;
}

// Assuming entries_from_R do not exceed n/k (does not have overflow cut_matrix)
// the meaning of entries_from_R differs when foward flag changes
uint64_t Emulator::get_probe_cost(uint32_t & m_r, uint32_t entries_from_R, uint32_t offset, uint32_t step_size, std::vector<uint32_t> & SumSoFar, const Params & params, Cut** cut_matrix) {
    if (entries_from_R >= SumSoFar.size()) entries_from_R = SumSoFar.size() - 1;
    if (entries_from_R <= offset) {
        m_r = 0;
        return 0;
    }
    uint32_t curr_steps = ceil((entries_from_R - offset)*1.0/step_size) - 1;
    while(cut_matrix[curr_steps][m_r].cost == UINT64_MAX && m_r >= 1) m_r--;
    if (m_r == 0) return cal_cost(offset, entries_from_R, SumSoFar);
    uint32_t lastPos =     cut_matrix[curr_steps][m_r].lastPos;
    uint32_t num_passes = ceil((entries_from_R - lastPos + 1)*1.0/step_size);
    uint64_t lastCost = 0;
    if (lastPos > offset) {
        uint32_t prev_steps = (uint32_t)ceil((lastPos - offset - 1)*1.0/step_size) - 1;
        // it is possible that cutting until the last position does not lead to decreasing partition (typically happen when the last partition is not ful-filled)
        if (prev_steps == curr_steps) {
            lastCost = cut_matrix[prev_steps][m_r].cost;
        } else {
            lastCost = cut_matrix[prev_steps][m_r - 1].cost;
        }
    }
    return lastCost + cal_cost(lastPos, entries_from_R, SumSoFar);
}

uint32_t Emulator::est_best_num_partitions(uint32_t & num_of_in_memory_partitions, uint32_t & num_of_random_in_memory_entries, double a, double b, double c, double hashtable_fulfilling_percent) {
    if (b > c*a) {
        num_of_in_memory_partitions = 1;
        num_of_random_in_memory_entries = c;
        return 1;
    }
    double t = min(floor(pow(b, 2)/(ceil(4*a*c))), b);
    double delta = pow(b + t, 2) - ceil(t*4*a*c);
    while (delta < 0 && t > 0) {
        delta = pow(b + t, 2) - ceil(t*4*a*c);
        t--;
    }
    if (delta < 0 || t == 0) {
        num_of_in_memory_partitions = 0;
        num_of_random_in_memory_entries = 0;
        return b;
    } else {
        num_of_in_memory_partitions = t;
        num_of_random_in_memory_entries = (uint32_t)(round((b + t + sqrt(delta))*0.5/a));
        uint32_t best_num_partitions = ceil(2*t*a*c/(b + t + sqrt(delta)));
        // compare against min(20, ceil((|R|*F-B)/(B-1)))
        uint32_t tmp_num_total_partitions = max(20.0, ceil((c*a-b)/(b-1)));

        while (ceil(c*a*1.0/tmp_num_total_partitions) > hashtable_fulfilling_percent*b) {
            tmp_num_total_partitions++;
        }
        tmp_num_total_partitions = std::min(tmp_num_total_partitions, (uint32_t)(b));
        uint32_t tmp_num_of_in_memory_partitions = floor((b - tmp_num_total_partitions)*1.0/(a*c/tmp_num_total_partitions - 1));
        uint32_t tmp_num_of_random_in_memory_entries = floor(c*1.0/tmp_num_total_partitions*tmp_num_of_in_memory_partitions);
        if (tmp_num_of_random_in_memory_entries > num_of_random_in_memory_entries) {
            num_of_in_memory_partitions = tmp_num_of_in_memory_partitions;
            num_of_random_in_memory_entries = tmp_num_of_random_in_memory_entries;
            return tmp_num_total_partitions;
        } else {
            return best_num_partitions;
        }
    }
}

inline double get_upper_bounded_percentage_with_chernoff_bound(double delta, double expectation) {
    return pow(exp(delta)/pow(delta+1, delta+1), expectation);
}

inline uint64_t overest_probe_cost_use_chernoff_bound(double floated_num_passes, double entries_from_R, double entries_from_S, uint64_t step_size) {
    double ceil_num_passes = ceil(floated_num_passes);
    if (floated_num_passes == ceil_num_passes) return (uint64_t) (round) ((floated_num_passes + 0.5)*entries_from_S + entries_from_R);
    double delta = ceil_num_passes/floated_num_passes - 1.0;
    double upper_bound_overflowed_partition_percentage = get_upper_bounded_percentage_with_chernoff_bound(delta, floated_num_passes*step_size);
    return (uint64_t)(round((ceil_num_passes+1)*upper_bound_overflowed_partition_percentage*entries_from_S  + 
                 ceil_num_passes*(1.0 - upper_bound_overflowed_partition_percentage)*entries_from_S + entries_from_R));
}

uint64_t Emulator::est_probe_cost(uint32_t & num_of_in_memory_partitions, uint32_t & num_of_random_in_memory_entries, uint32_t m_r, uint32_t entries_from_S, uint32_t entries_from_R, uint32_t step_size, bool one_page_used_for_hybrid_join, const Params & params){
    double selected_entries_from_R = entries_from_R*params.left_selection_ratio;
    double selected_entries_from_S = entries_from_S*params.right_selection_ratio;
    if (params.hybrid) {
        uint32_t available_pages = m_r - (one_page_used_for_hybrid_join ? 0 : 1);
        est_best_num_partitions(num_of_in_memory_partitions, num_of_random_in_memory_entries,  (FUDGE_FACTOR*params.left_E_size)*1.0/DB_PAGE_SIZE, available_pages ,entries_from_R*params.left_selection_ratio, params_.hashtable_fulfilling_percent);
        if (num_of_in_memory_partitions > 0) {
            selected_entries_from_S = selected_entries_from_S - floor((num_of_random_in_memory_entries*1.0/selected_entries_from_R)*selected_entries_from_S);
            selected_entries_from_R = selected_entries_from_R - floor(num_of_random_in_memory_entries);
        }
    }
    
    double entries_per_partition = selected_entries_from_R/m_r;
    uint32_t tmp_num_passes = ceil(selected_entries_from_R/m_r/step_size);
    double tmp_floated_num_passes = selected_entries_from_R/m_r/step_size;
    double upper_bound_overflowed_partition_percentage = 0.0;
    bool no_rounded_hash = false;
    if(params.rounded_hash){
        if(ceil(tmp_floated_num_passes/params.hashtable_fulfilling_percent) - tmp_num_passes >= 1){
            no_rounded_hash = true;
        }else{
            tmp_num_passes = ceil(selected_entries_from_R/m_r/(step_size*params.hashtable_fulfilling_percent)) ;
        }
    }
        
    if(tmp_num_passes > 2 + params.seqwrite_seqread_ratio && (ceil(entries_per_partition/left_entries_per_page/2/(params.B - 1)) + ceil(selected_entries_from_S/m_r/right_entries_per_page/2/(params.B - 1)) < params.B - 1)){
        // applying SMJ for partition-wise joins
        return (uint64_t)ceil((2 + params.seqwrite_seqread_ratio)*(selected_entries_from_S + selected_entries_from_R));
    }else if(tmp_num_passes > 2 + params.randwrite_seqread_ratio){
        // applying recursive GHJ if m_r is too small
        return est_GHJ_cost(selected_entries_from_R, selected_entries_from_S);
    }else{
        // When SMJ and GHJ are selected to execute partition-wise join, there is no much difference in terms of the cost estimation no matter whether a partition is overflowed or not
        // Now, we will apply NBJ in this condition branch, and for nearly fulfilled partitions, we use Chernoff bound to overestimate the cost in case some partitions are overflowed
        // due to the random assignment
        uint32_t divider = 0;
        uint32_t remainder_entries = 0;
        if(params.rounded_hash){
            if(tmp_num_passes > 2 + params.seqwrite_seqread_ratio && (ceil(entries_per_partition/left_entries_per_page/2/(params.B - 1)) + ceil(selected_entries_from_S/m_r/right_entries_per_page/2/(params.B - 1)) < params.B - 1)){
                if(no_rounded_hash) {
                    double upper_bound_overflowed_partition_percentage = get_upper_bounded_percentage_with_chernoff_bound(tmp_num_passes/tmp_floated_num_passes - 1, tmp_floated_num_passes*step_size);
                    remainder_entries = m_r*(1.0 - upper_bound_overflowed_partition_percentage)*ceil(selected_entries_from_R/m_r);
                    tmp_num_passes++;
                } else {
                    divider = ceil(selected_entries_from_R/((1+params.seqwrite_seqread_ratio)*(step_size*params.hashtable_fulfilling_percent)));
                    remainder_entries = (m_r - divider%m_r)*floor(divider/m_r)*((1+params.seqwrite_seqread_ratio)*step_size*params.hashtable_fulfilling_percent);
                }
            
                return (uint64_t)((2+params.seqwrite_seqread_ratio)*(selected_entries_from_R - remainder_entries) + 
                   (2+params.seqwrite_seqread_ratio)*(selected_entries_from_R - remainder_entries)*1.0/selected_entries_from_R*selected_entries_from_S+ 
                   ((tmp_num_passes - 1) * remainder_entries*1.0 + tmp_num_passes * (selected_entries_from_R - remainder_entries))/selected_entries_from_R*selected_entries_from_S);
	    } else if (tmp_num_passes >= 2 + params.randwrite_seqread_ratio ) {
		    return (2 + params.randwrite_seqread_ratio)*(selected_entries_from_S + selected_entries_from_R);
            }else if(!no_rounded_hash){
                divider = ceil(selected_entries_from_R/(step_size*params.hashtable_fulfilling_percent));
                remainder_entries = (m_r - divider%m_r)*floor(divider/m_r)*step_size*params.hashtable_fulfilling_percent;
                if(divider%m_r == 0 || divider == 1){
                    if (tmp_floated_num_passes/ceil(tmp_floated_num_passes) >= params.hashtable_fulfilling_percent) {
                        return overest_probe_cost_use_chernoff_bound(tmp_floated_num_passes, selected_entries_from_R, selected_entries_from_S, step_size);
                    } else {
                        return (uint64_t)(ceil(tmp_num_passes*selected_entries_from_S + selected_entries_from_R));
                    }
                }
                return (uint64_t)round(selected_entries_from_R + ((selected_entries_from_R - remainder_entries)*1.0*tmp_num_passes + 
                remainder_entries*(tmp_num_passes - 1))*1.0/selected_entries_from_R*selected_entries_from_S);
             } else {
                return overest_probe_cost_use_chernoff_bound(tmp_floated_num_passes, selected_entries_from_R, selected_entries_from_S, step_size);
            }
        }
        
        return (uint64_t)(round(tmp_num_passes*selected_entries_from_S) + selected_entries_from_R);
    }
}

uint64_t Emulator::est_GHJ_cost(double entries_from_R, double entries_from_S) {
    uint64_t cost = 0;
    double num_passes = 0;
    uint64_t ceil_num_passes = 0;
    uint64_t remainder_entries = 0;
    uint32_t m = params_.B - 1;
    double entries_per_partition = entries_from_R/m;
    cost += (1 + params_.randwrite_seqread_ratio)*(entries_from_R + entries_from_S) + 3*m;
    while(true) {
        num_passes = entries_per_partition/step_size;
        if (ceil(num_passes) > 2 + params_.randwrite_seqread_ratio) {
            cost += (1 + params_.randwrite_seqread_ratio)*(entries_from_R + entries_from_S) + 2*m;
        } else if (ceil(num_passes/params_.hashtable_fulfilling_percent) - ceil(num_passes) >= 1) {
            cost += overest_probe_cost_use_chernoff_bound(num_passes, entries_from_R, entries_from_S, step_size);
            return cost;
        } else if (params_.rounded_hash) {
            ceil_num_passes = (uint64_t) ceil(entries_per_partition/step_size/params_.hashtable_fulfilling_percent);
            if(ceil_num_passes%m == 0 || ceil_num_passes == 1) {
                cost += ceil_num_passes*entries_from_S + entries_from_R;
            } else {
                remainder_entries = (m - ceil_num_passes%m)*floor(ceil_num_passes/m)*step_size*params_.hashtable_fulfilling_percent;
                cost += (uint64_t) round((entries_from_R - remainder_entries)*1.0/entries_from_R*entries_from_S*ceil_num_passes + 
                remainder_entries*(ceil_num_passes - 1)*1.0/entries_from_R*entries_from_S)+ entries_from_R;
            }
            
            return cost;
        } else {
            cost += overest_probe_cost_use_chernoff_bound(num_passes, entries_from_R, entries_from_S, step_size);
            return cost;
        }
        entries_per_partition = entries_per_partition/(params_.B - 1);
    }
}

uint64_t Emulator::cal_cost(uint32_t start_idx, uint32_t end_idx, std::vector<uint32_t> & SumSoFar) {
    uint32_t tmp_end_idx = end_idx;
    uint32_t tmp_start_idx = start_idx;
    if(params_.left_selection_ratio != 1){
        tmp_end_idx = (uint32_t) ceil(end_idx*1.0/params_.left_selection_ratio);
          if(tmp_end_idx > SumSoFar.size() - 1) tmp_end_idx = SumSoFar.size();
          tmp_start_idx = (uint32_t) floor(start_idx*1.0/params_.left_selection_ratio);
    }
    uint64_t selected_entries_from_S = static_cast<uint64_t>((SumSoFar[tmp_end_idx] - SumSoFar[max(tmp_start_idx,1U)-1])*params_.right_selection_ratio);
    uint64_t selected_entries_from_R = end_idx - start_idx+1;
    double num_passes = ceil(selected_entries_from_R*1.0/step_size);

    double est_sorted_runs_R_p0 = ceil(selected_entries_from_R*1.0/left_entries_per_page/1.75/(params_.B-1));
    double est_sorted_runs_S_p0 = ceil(selected_entries_from_S*1.0/right_entries_per_page/1.75/(params_.B-1));
    if (num_passes > 2 + params_.seqwrite_seqread_ratio) {
        uint64_t smj_cost = (uint64_t)((selected_entries_from_S + selected_entries_from_R)*(2 + params_.seqwrite_seqread_ratio));
        if (est_sorted_runs_R_p0 + est_sorted_runs_S_p0 < params_.B - 1) {
        return smj_cost; 
        }
        uint64_t ghj_cost = est_GHJ_cost(selected_entries_from_R, selected_entries_from_S);
        while (est_sorted_runs_R_p0 + est_sorted_runs_S_p0 > params_.B - 1 && smj_cost < ghj_cost) { 
            if (est_sorted_runs_S_p0 + ceil(est_sorted_runs_R_p0*1.0/(params_.B - 1)) < params_.B - 1) {
               est_sorted_runs_R_p0 = ceil(est_sorted_runs_R_p0*1.0/(params_.B - 1));
              smj_cost += (uint64_t)(ceil((1 + params_.seqwrite_seqread_ratio)*selected_entries_from_R)); 
            } else if (est_sorted_runs_R_p0 + ceil(est_sorted_runs_S_p0*1.0/(params_.B - 1)) < params_.B - 1) {
               est_sorted_runs_S_p0 = ceil(est_sorted_runs_S_p0*1.0/(params_.B - 1));
              smj_cost += (uint64_t)(ceil((1 + params_.seqwrite_seqread_ratio)*selected_entries_from_S)); 
            } else {
               est_sorted_runs_S_p0 = ceil(est_sorted_runs_S_p0*1.0/(params_.B - 1));
               est_sorted_runs_R_p0 = ceil(est_sorted_runs_R_p0*1.0/(params_.B - 1));
              smj_cost += (uint64_t)(ceil((1 + params_.seqwrite_seqread_ratio)*(selected_entries_from_S + selected_entries_from_R))); 
            }
        }
        return min(smj_cost, ghj_cost);
    } else {
        return (uint64_t)(selected_entries_from_S*num_passes + selected_entries_from_R) ;
    }
    
}

void Emulator::populate_cut_matrix(uint32_t n, uint32_t m,  uint32_t offset, bool appr_flag, std::vector<uint32_t> & SumSoFar, Params & params, uint64_t & min_cost, uint32_t & num_partitions, Cut** cut_matrix) {
    if (n < offset) {
        min_cost = 0;
        num_partitions = 0;
        return;
    }
    uint32_t num_elements = n - offset;
    uint32_t tmp_start, tmp_end, tmp;
    uint32_t tmp_pos;
    uint64_t tmp_cost;

    uint32_t num_steps = (uint32_t)floor(num_elements*1.0/step_size);
    uint32_t exact_pos_i, exact_pos_k, tmp_k;  
    uint32_t j_lower_bound = 2;
    uint32_t j_upper_bound = m - 1;
    
    for(auto i = 0; i <= num_steps; i++){
        // calculate cost for one partition (the implementation may differ between the approximate version and the optimal version)
        cut_matrix[i][1].cost = cal_cost(offset,  floor(min((i + 1)*step_size + offset, n)), SumSoFar); 
        cut_matrix[i][1].lastPos = offset;
        for (auto j = 2; j <= m; j++) {
            cut_matrix[i][j].cost = UINT64_MAX;
        }
    }

    if (m == 1 || num_steps == 0) {
        num_partitions = 1;
        return;
    }

    // start dynamic programming
    for(auto i = 1; i <= num_steps; i++){
        exact_pos_i = min((i+1)*step_size + offset, n);
        j_upper_bound = min((uint32_t)i + 1, m - 1);
        for(auto j = 2; j <= j_upper_bound; j++){
            // Pigeonhole optimization: the partition size between tmp_start and exact_pos_i should be no more than
            // the minimum partition size in the rest partitions (off by step_size)
            // tmp_start cannot be too small since the partition size has to be smaller than some threshold
            tmp = static_cast<uint32_t>(ceil((num_elements + offset - exact_pos_i)*1.0/(m - j)));
            if(tmp%step_size != 0){
                if(ceil(tmp*1.0/step_size)*step_size + offset < exact_pos_i){
                    // we use ceil since we use a weak version of pigeonhole optimization (off by step_size)
                    tmp_start = exact_pos_i - ceil(tmp*1.0/step_size)*step_size;
                }else{
                    // no pigeonhole optimization can be applied here because tmp is too large
                    // Any partition for exact_pos_i with j partitions could be fine
                    tmp_start = step_size + offset;
                }
            }else{
                if(tmp + step_size < exact_pos_i){
                    // off by step_size (a weak version of pigeonhole optimization)
                    tmp_start = max(exact_pos_i - tmp - step_size, step_size + offset);
                }else{
                    // no pigeonhole optimization can be applied here
                    // similar reason as above
                    tmp_start = step_size + offset;
                }
            }
            // Pigenhole optimization: there exists a partition whose size is larger than
            // the average partition size of all the partitions for top exact_pos_i elements with j partitions
            // tmp_end cannot be too large since the last partition has to be large than some threshold
            tmp = static_cast<uint32_t> (floor(((exact_pos_i - offset)*(1.0 - 1.0/j))));
            if(tmp%step_size != 0){
                tmp_end = ceil(tmp*1.0/step_size)*step_size + offset;
            }else{
                // off by step_size (can only use a weak version of pigenhole optimization)
                tmp_end = tmp + step_size + offset;
            }
            if(tmp_end >= exact_pos_i) tmp_end = exact_pos_i; 
            if(tmp_end < tmp_start) continue;
            for(auto k = 0; tmp_start + k*step_size <= tmp_end; k++){
                exact_pos_k = tmp_start + k*step_size;
                tmp_k = ceil((exact_pos_k - offset)*1.0/step_size) - 1;
                // skip because we have to cut at the position that the new partition is smaller than the last one    
                // (again, a weak version of pigenhole optimization)
                if(cut_matrix[tmp_k][j-1].cost == UINT64_MAX || exact_pos_i + cut_matrix[tmp_k][j-1].lastPos + step_size < 2*exact_pos_k) continue;
                tmp_cost = cal_cost(exact_pos_k+1,exact_pos_i, SumSoFar) + cut_matrix[tmp_k][j-1].cost;
                if(tmp_cost < cut_matrix[i][j].cost){
                    cut_matrix[i][j].cost = tmp_cost;
                    cut_matrix[i][j].lastPos = exact_pos_k+1;
                }
            }
        }
    }
    // a weak version of pigeonhole optimization
    tmp_end = static_cast<uint32_t> (floor((num_elements*(1.0 - 1.0/m))));
    if(tmp_end%step_size != 0){
        tmp_end = ceil(tmp_end*1.0/step_size)*step_size + offset;
    }else{
        // off by step_size (can only use a weak version of pigenhole optimization)
        tmp_end += step_size + offset;
    }
    tmp_end = min(tmp_end, n);

    uint32_t max_partitions; 
    num_partitions = 0;
    uint64_t tmp_cost1;
    uint32_t tmp_num_passes;
  
    for(exact_pos_k = offset + step_size; exact_pos_k <= tmp_end; exact_pos_k+=step_size){
        tmp_cost = UINT64_MAX;
        tmp_k = ceil((exact_pos_k - offset)*1.0/step_size) - 1;
        // find the minimum partition that achieves the minimum cost
        for(auto j = std::min(tmp_k+1, m-1); j >= 1; j--){
               if(cut_matrix[tmp_k][j].cost != UINT64_MAX) {
                tmp_cost1 = cut_matrix[tmp_k][j].cost;
                if(tmp_cost1 != tmp_cost && tmp_cost != UINT64_MAX){
                    break;
                   }
                tmp_cost = tmp_cost1;
                max_partitions = j;
               } else if (tmp_cost != UINT64_MAX) {
                break;
            }
        }
        // skip due to the weak pigeonhole optimization
        if (n + cut_matrix[tmp_k][max_partitions].lastPos + step_size < 2*exact_pos_k) continue;
        tmp_cost = cal_cost(exact_pos_k+1,n, SumSoFar) + cut_matrix[tmp_k][max_partitions].cost;
        if(tmp_cost < min_cost){
            num_partitions = max_partitions + 1;
            cut_matrix[num_steps][num_partitions].cost = tmp_cost;
            cut_matrix[num_steps][num_partitions].lastPos = exact_pos_k+1;
            min_cost = tmp_cost;
        }
    }
    //std::cout << cut_matrix[num_steps][m].cost << std::endl;
    //std::cout << cut_matrix[num_steps][m].lastPos << std::endl;
}

void Emulator::get_cutting_pos(uint32_t n, uint32_t m, uint32_t offset, uint32_t step_size, std::vector<uint32_t> & cut_pos, Cut** cut_matrix, Params & params) {
    cut_pos.clear();
    uint32_t num_steps = ceil((n - offset)*1.0/step_size) - 1;
    uint32_t lastPos = n;
    for(auto j = 0U; j < m; j++){
        cut_pos.push_back(lastPos);    
        if (lastPos == offset) break;
        lastPos = cut_matrix[(uint32_t)ceil((lastPos - offset)*1.0/step_size) - 1][m - j].lastPos - 1;
    }
}

void Emulator::populate_prioritized_keys(uint32_t best_in_mem_entries, std::vector<uint32_t> & cut_pos, std::vector<std::string> & keys,
std::vector<std::pair<uint32_t, uint32_t> > & key_multiplicity_to_be_sorted, std::unordered_map<std::string, uint16_t> & partitioned_keys,
std::unordered_set<std::string> & in_memory_keys) {
    partitioned_keys.clear();
    in_memory_keys.clear();
    if (best_in_mem_entries > 0) {
        for (auto i = 0; i < best_in_mem_entries; i++) {
            in_memory_keys.insert(keys[key_multiplicity_to_be_sorted[i].second]);
        }
    }

    if (cut_pos.size() == 0) return;

    uint32_t num_in_disk_partitions = cut_pos.size();
    cut_pos.push_back(best_in_mem_entries);
    for(auto i = 0; i < num_in_disk_partitions; i++){
        for (int j = cut_pos[i]; j > cut_pos[i + 1]; j--) {
            partitioned_keys.emplace(keys[key_multiplicity_to_be_sorted[j-1].second], i);
        }
    }
    //std::cout << partitioned_keys.size() << std::endl;
}

std::pair<uint32_t, uint32_t> Emulator::get_partitioned_keys(std::vector<std::string> & _keys, std::vector<uint32_t> & _key_multiplicity, std::unordered_map<std::string, uint16_t> & partitioned_keys, std::unordered_set<std::string> & in_memory_keys, bool & turn_on_NBJ, bool appr_flag){
    uint32_t left_entries_per_page = params_.page_size/params_.left_E_size;
    uint32_t right_entries_per_page = params_.page_size/params_.right_E_size;

    uint32_t hash_map_size = get_hash_map_size(params_.k, params_.join_key_size);
    uint32_t m_r = 0;
    uint32_t num_remaining_keys;
    uint32_t num_remaining_matches;

    uint64_t summed_match = 0;
    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;
    for(uint32_t i = 0; i < _keys.size(); i++){
    if(key_multiplicity[i] == 0) continue;
        summed_match += _key_multiplicity[i];
        key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity[i], i));
    }
    std::sort(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    std::reverse(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());

    uint32_t n = static_cast<uint32_t>(round(key_multiplicity_to_be_sorted.size()*params_.left_selection_ratio));    
    uint32_t m = std::max(1U, std::min(params_.num_partitions, (uint32_t)ceil(n*1.0/step_size))); // dynamic programming only operates when m <= n/step_size
    if(appr_flag){
        m = min((uint32_t)ceil(params_.k*params_.left_selection_ratio/step_size), params_.B - 1);
        num_remaining_matches = static_cast<uint32_t>(round((params_.right_table_size - summed_match)*params_.right_selection_ratio));
    }
    

    std::vector<uint32_t> SumSoFar = std::vector<uint32_t> (key_multiplicity_to_be_sorted.size()+1, 0U);
    for(auto i = 1U; i <= key_multiplicity_to_be_sorted.size(); i++){
        SumSoFar[i] += SumSoFar[i-1] + key_multiplicity_to_be_sorted[i-1].first;
    }
    
    
    uint32_t num_steps = floor(n*1.0/step_size);
    Cut** cut_matrix = new Cut*[num_steps+1];
    memset(cut_matrix, 0, (num_steps+1)*sizeof(Cut*));
    for(auto i = 0; i < num_steps; i++){
        cut_matrix[i] = new Cut[m + 1];
        cut_matrix[i][1].lastPos = 0;
    }
    cut_matrix[num_steps] = new Cut[m + 1];
    uint64_t nbj_cost = ((uint64_t)ceil(params_.left_table_size*1.0/step_size)-1)*ceil(params_.right_table_size/right_entries_per_page);
    uint64_t min_cost = nbj_cost;
    if(min_cost == 0U) {
        turn_on_NBJ = true;
        return make_pair(0U, 0U);
    }
    // compare min cost against NBJ

    uint32_t num_partitions = 0;
    uint32_t tmp_num_partitions = 0;
    uint32_t remaining_pages = 0;

    uint32_t max_pages_for_skew_keys_partition = 0;
    if (params_.hybrid) {
	max_pages_for_skew_keys_partition = min(params_.B - 2, (uint32_t)ceil(params_.k*params_.left_selection_ratio/left_entries_per_page/FUDGE_FACTOR));
    }

    uint32_t final_passes = 0;
    uint32_t final_k = 0;
    
    // calculate best num_in_mem_entries or best special spilled keys for hybrid hash or approximate version
    uint32_t tmp_exact_pos_k, exact_pos_k, tmp_k; 
    
    uint32_t tmpk_hash_map_size;
    uint32_t tmp_num_remaining_keys;
    uint64_t tmp_cost = UINT64_MAX;
    uint64_t tmp_probe_cost, probe_cost_in_min_cost;

    num_partitions = 0;

    uint64_t partition_cost = 0;
    uint64_t delta_partition_cost_UB = 0;
    uint32_t num_in_mem_skew_entries = 0;
    uint32_t best_num_in_mem_skew_entries = 0;
    uint32_t num_of_random_in_mem_partitions = 0;
    uint32_t best_num_of_random_in_mem_partitions = 0;
    uint32_t num_of_est_random_in_mem_entries = 0;
    std::vector<uint32_t> cut_pos;
    for(uint32_t pages_for_skew_keys_partition = 0; pages_for_skew_keys_partition <= max_pages_for_skew_keys_partition; pages_for_skew_keys_partition++) {
	num_in_mem_skew_entries = min(min((uint32_t)floor(floor(pages_for_skew_keys_partition*DB_PAGE_SIZE/FUDGE_FACTOR)/params_.left_E_size), n), (uint32_t)floor((params_.B - 2)*left_entries_per_page*1.0/FUDGE_FACTOR));

        delta_partition_cost_UB = ceil(num_in_mem_skew_entries*params_.left_selection_ratio/left_entries_per_page) + ceil(SumSoFar[num_in_mem_skew_entries]*params_.right_selection_ratio/right_entries_per_page);
        partition_cost = params_.randwrite_seqread_ratio*
            (ceil((params_.left_table_size - num_in_mem_skew_entries)*params_.left_selection_ratio/left_entries_per_page) + 
              ceil((params_.right_table_size - SumSoFar[num_in_mem_skew_entries])*params_.right_selection_ratio)/right_entries_per_page);
        tmp_cost = UINT64_MAX; // the minimum join cost for n - offset entries with m partitions

        // one for input, at least one page one for partitioning the rest entries
        // when we allow data allocated in memory, one more output page required when partitioning the relation S
        remaining_pages = params_.B - 1 - pages_for_skew_keys_partition - ((pages_for_skew_keys_partition > 0) ? 1 : 0);
        if (n > num_in_mem_skew_entries && remaining_pages == 0) continue;

        // for num_in_mem_skew_entries != 0, cut_matrix has been populated for pruning
        if (n > 2*step_size && remaining_pages <= num_steps && num_in_mem_skew_entries != 0) {
            tmp_cost = cut_matrix[(uint32_t)ceil((n - step_size)*1.0/step_size) - 1][remaining_pages].cost;
            if (tmp_cost != UINT64_MAX && ceil((tmp_cost + SumSoFar[step_size] - SumSoFar[num_in_mem_skew_entries])*1.0/right_entries_per_page) 
              >= probe_cost_in_min_cost + delta_partition_cost_UB) {
                // delta_partition_cost_UB is the upper bound of the differnce of partition cost between the best so-far partition cost and the current one,
                // the baseline cost is calculated assuming nothing is cached in memory and thus the partition cost in that case is the upper bound of the differnce of partition cost
                continue;
            }
        }
        // for every possible n - offset entries, we populate the minimum cost in cut matrix
        tmp_cost = UINT64_MAX;
        populate_cut_matrix(n, min(remaining_pages,m), num_in_mem_skew_entries, appr_flag,  SumSoFar, params_, tmp_cost, tmp_num_partitions, cut_matrix);
        
        
        if (!appr_flag) {
            if (tmp_num_partitions < remaining_pages && params_.hybrid) {
                pages_for_skew_keys_partition += remaining_pages - tmp_num_partitions;
                // recalculate partition cost because we decide to move more entries in memory
                num_in_mem_skew_entries = min((uint32_t)floor(pages_for_skew_keys_partition*left_entries_per_page/FUDGE_FACTOR), n); 
                partition_cost = params_.randwrite_seqread_ratio*
            (ceil((params_.left_table_size - num_in_mem_skew_entries)*params_.left_selection_ratio/left_entries_per_page) + 
              ceil((params_.right_table_size - SumSoFar[num_in_mem_skew_entries])*params_.right_selection_ratio)/right_entries_per_page);

                tmp_cost = UINT64_MAX;
                // repopulate cut matrix
                populate_cut_matrix(n, min(remaining_pages,m), num_in_mem_skew_entries, false,  SumSoFar, params_, tmp_cost, tmp_num_partitions, cut_matrix);
            }
            
            tmp_probe_cost = ceil(get_probe_cost(tmp_num_partitions, n, (uint32_t)floor(num_in_mem_skew_entries*params_.left_selection_ratio),
                step_size, SumSoFar, params_, cut_matrix)*1.0/right_entries_per_page);
            tmp_cost = partition_cost + tmp_probe_cost;
            if (tmp_cost < min_cost) {
                min_cost = tmp_cost;
                probe_cost_in_min_cost = tmp_probe_cost;
                get_cutting_pos(n, tmp_num_partitions, num_in_mem_skew_entries, step_size, cut_pos, cut_matrix, params_);
                best_num_in_mem_skew_entries = num_in_mem_skew_entries;
                // to implement get cutting pos here
                num_partitions = tmp_num_partitions + ((pages_for_skew_keys_partition > 0 ) ? 1 : 0);
            }
        } else {
        
            // enumerate all other possible number of prioritized spilled entries (small partition in disk)
            exact_pos_k = 0;
            while(exact_pos_k < n - num_in_mem_skew_entries) {
                tmp_k = ceil(exact_pos_k*1.0/step_size);
                tmpk_hash_map_size = get_hash_map_size(exact_pos_k + num_in_mem_skew_entries, params_.join_key_size);
                // at least one page for partitioning
                if(tmpk_hash_map_size + 1 > remaining_pages) break; 
            
                for(uint32_t j = min(tmp_k, 1U); j + tmpk_hash_map_size + 1 <= remaining_pages && j <= tmp_k; j++){
                    // j is the number of partitions for prioritized spilled entries
                    tmp_exact_pos_k = (uint32_t)ceil((exact_pos_k + num_in_mem_skew_entries)/params_.left_selection_ratio);
                    tmp_num_remaining_keys = params_.left_table_size - tmp_exact_pos_k;
                    tmp_num_partitions = j;
                    if (tmp_num_partitions == 0 || exact_pos_k == 0) {
                        tmp_num_partitions = 0;
                        tmp_cost = partition_cost;
                        m_r = remaining_pages - get_hash_map_size(num_in_mem_skew_entries, params_.join_key_size);
                        tmp_probe_cost = 0;
                    } else {
                        // change get_probe_cost to adapt exact_pos_k
                        // tmp_num_partitions might change when calling get_probe_cost function
                        tmp_probe_cost = ceil(get_probe_cost(tmp_num_partitions, (uint32_t)floor(tmp_exact_pos_k*params_.left_selection_ratio),  (uint32_t)round(num_in_mem_skew_entries*params_.left_selection_ratio), step_size, SumSoFar, params_, cut_matrix)/right_entries_per_page);
                        tmp_cost = partition_cost + tmp_probe_cost; 
                        // get the remaining pages as partitions for remaining keys
                        m_r = remaining_pages - tmpk_hash_map_size - tmp_num_partitions;    
                    }
                    num_of_random_in_mem_partitions = 0;
                    if (tmp_exact_pos_k == 22086 && num_in_mem_skew_entries == 0 && tmp_num_partitions == 15) {
			    tmp_exact_pos_k++;
			    tmp_exact_pos_k--;
		    }
                    tmp_probe_cost += ceil(est_probe_cost(num_of_random_in_mem_partitions, num_of_est_random_in_mem_entries, m_r, params_.right_table_size - SumSoFar[tmp_exact_pos_k], tmp_num_remaining_keys, step_size, pages_for_skew_keys_partition > 0, params_)/right_entries_per_page);
                    tmp_cost += tmp_probe_cost;
                    
                    if (num_of_random_in_mem_partitions > 0) {
                        tmp_cost -= params_.randwrite_seqread_ratio*(
                            floor(num_of_est_random_in_mem_entries/left_entries_per_page) + 
                            floor(num_of_est_random_in_mem_entries*params_.right_selection_ratio/(tmp_num_remaining_keys*params_.left_selection_ratio)*(params_.right_table_size - SumSoFar[tmp_exact_pos_k])/right_entries_per_page)
                            );
                    }
                    
                    if(tmp_cost < min_cost){
                        min_cost = tmp_cost;
                        probe_cost_in_min_cost = tmp_probe_cost;
                        best_num_in_mem_skew_entries = num_in_mem_skew_entries;
                        best_num_of_random_in_mem_partitions = num_of_random_in_mem_partitions;
                        get_cutting_pos(tmp_exact_pos_k, tmp_num_partitions, num_in_mem_skew_entries, step_size, cut_pos, cut_matrix, params_);
                        num_partitions = tmp_num_partitions + (pages_for_skew_keys_partition > 0 ? 1 : 0);
                        final_passes = ceil(tmp_num_remaining_keys*params_.left_selection_ratio/m_r/step_size);
                        final_k = (uint32_t)round(exact_pos_k/params_.left_selection_ratio);
                    }
                }

                tmp_exact_pos_k = get_max_hash_map_entries(tmpk_hash_map_size, params_.join_key_size);
                if (tmp_exact_pos_k == exact_pos_k) {
                    tmp_exact_pos_k = get_max_hash_map_entries(tmpk_hash_map_size+1, params_.join_key_size);
                }
                exact_pos_k += std::min(std::min(tmp_exact_pos_k - exact_pos_k, step_size - exact_pos_k%step_size), n - num_in_mem_skew_entries - exact_pos_k);
            }
        }

        if (num_in_mem_skew_entries == 0 && min(remaining_pages, m) > 1 && params_.hybrid) {
            // calculate the best cost with entries (off by step_size) and fewer partitions for future pruning
            tmp_cost = UINT64_MAX;
            populate_cut_matrix(n, min(remaining_pages, m) - 1, step_size, appr_flag,  SumSoFar, params_, tmp_cost, tmp_num_partitions, cut_matrix);
        }
    }

    if (min_cost == nbj_cost) {
        turn_on_NBJ = true;
    }

    
    
    if(params_.debug && (params_.hybrid || appr_flag)){ 
        std::cout << "estimated minimum cost: " << min_cost 
          << " with #partitions=" << num_partitions << " and #in_mem_entries=" << best_num_in_mem_skew_entries << std::endl;
        std::cout << "Final #passes for remaining keys: " << final_passes << "\t final k " << final_k << std::endl;
    }

    if (num_partitions > 0) {
        populate_prioritized_keys(best_num_in_mem_skew_entries, cut_pos, keys, key_multiplicity_to_be_sorted, partitioned_keys, in_memory_keys);
    }
    
    for(auto i = 0; i < num_steps+1; i++){
        delete[] cut_matrix[i];
    }
    delete[] cut_matrix;

    if(!params_.debug){ // emulating switching the memory context
       keys.clear();
       key_multiplicity.clear();
    }
    return std::make_pair(num_partitions, best_num_of_random_in_mem_partitions);
}





void Emulator::get_emulated_cost_MatrixDP(){

    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    load_key_multiplicity(keys, key_multiplicity);
    std::vector<uint32_t> idxes = std::vector<uint32_t> (params_.left_table_size, 0U);
    for(auto i = 0; i < params_.left_table_size; i++){
    idxes[i] = i;
    }
    get_emulated_cost_MatrixDP(idxes, params_.B, params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}

void Emulator::get_emulated_cost_MatrixDP(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){

    // only set filter condition of S for now in case of tpch Q12 query 
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = params_.tpch_q12_flag ? 1 : 0;

    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t num_passes_R = 0;
    num_passes_R = ceil(left_num_entries*1.0/step_size);
    // if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(left_num_entries/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(right_num_entries/right_entries_per_page)){
    //     if(depth != 0){
    //         probe_start = std::chrono::high_resolution_clock::now();
    //         get_emulated_cost_NBJ("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, left_num_entries, right_num_entries, true);
    //         probe_end = std::chrono::high_resolution_clock::now();
    //         probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
    //     }else{
    //         probe_start = std::chrono::high_resolution_clock::now();
    //         get_emulated_cost_NBJ(left_file_name, right_file_name, left_num_entries, right_num_entries, true);
    //         probe_end = std::chrono::high_resolution_clock::now();
    //         probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
    //     }
    //     return;
    // }
    std::unordered_map<std::string, uint16_t> partitioned_keys;
    std::unordered_set<std::string> in_memory_keys; 
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();

    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    // no random in-memory partition, so only the first partition number is required
    bool turn_on_NBJ = false;
    uint32_t num_partitions = (get_partitioned_keys(keys, key_multiplicity, partitioned_keys, in_memory_keys, turn_on_NBJ, false)).first;
    std::chrono::time_point<std::chrono::high_resolution_clock>  algo_end = std::chrono::high_resolution_clock::now();
    algo_duration += (std::chrono::duration_cast<std::chrono::microseconds>(algo_end - partition_start)).count();
    //std::cout << "matrix dp #partitions: " << partitions.size() << std::endl;
    if (turn_on_NBJ) {
        if(depth != 0){
                   probe_start = std::chrono::high_resolution_clock::now();
               get_emulated_cost_NBJ("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, left_num_entries, depth, R_filter_condition, S_filter_condition, true);
                   probe_end = std::chrono::high_resolution_clock::now();
                   probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
        }else{
                   probe_start = std::chrono::high_resolution_clock::now();
               get_emulated_cost_NBJ(left_file_name, right_file_name, left_num_entries, right_num_entries, depth, R_filter_condition, S_filter_condition, true);
                   probe_end = std::chrono::high_resolution_clock::now();
                   probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
        }
        return;
    }
 
    uint32_t in_disk_num_partitions = num_partitions - (in_memory_keys.size() ? 1: 0);
    if (in_memory_keys.size()) {
        std::cout << in_memory_keys.size() << " entries are cached in-memory (not-spilled to disk) in hybrid hash join" << "."<< std::endl;
    }
    std::vector<uint32_t> counter_R = std::vector<uint32_t> (in_disk_num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (in_disk_num_partitions, 0U);
    std::unordered_map<std::string, std::string> key2Rvalue;
    params_.num_partitions = num_partitions;

    partition_file(counter_R, partitioned_keys, in_memory_keys, key2Rvalue, num_partitions, 0, left_file_name, params_.left_E_size, params_.left_table_size, 0, params_.left_selection_ratio, &left_selection_seed, "part_rel_R/R", depth, R_filter_condition, true); 

    partition_file(counter_S, partitioned_keys, in_memory_keys, key2Rvalue, num_partitions, 0, right_file_name, params_.right_E_size, params_.right_table_size, 0, params_.right_selection_ratio, &right_selection_seed, "part_rel_S/S", depth, S_filter_condition, false); 
    
    if(params_.debug && num_partitions != 0){
        print_counter_histogram(partitioned_keys, key_multiplicity, keys, num_partitions);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);


    //probing
    uint32_t x = 0;
    bool SMJ_flag = false;
    for(auto i = 0; i < in_disk_num_partitions; i++){
    
    if(counter_R[i] == 0 || counter_S[i] == 0){
        
        // if(counter_R[i] != 0) 
        //     remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        // if(counter_S[i] != 0) 
        //     remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
        
        continue;
    }
        
    if(params_.debug && depth == 0){
        std::cout << "counter_R : " << counter_R[i] << " --- counter_S : " << counter_S[i] << std::endl;
    }
    num_passes_R = ceil(counter_R[i]*1.0/step_size);
    SMJ_flag = (!params_.no_smj_partition_wise_join) && ((ceil(counter_S[i]*1.0/(right_entries_per_page*2*(params_.B - 1))) + ceil(counter_R[i]*1.0/(left_entries_per_page*2*(params_.B - 1)))) < (params_.B - 1));
    if((num_passes_R <= 2 + params_.seqwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.seqwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page))|| ((num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page)) && !SMJ_flag)){
    //if(num_passes_R <= 1){
        
       if(params_.debug && depth == 0) std::cout << "NBJ" << std::endl;
        
            probe_start = std::chrono::high_resolution_clock::now();
        get_emulated_cost_NBJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1, 0, 0, true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
        //remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        //remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
    }else if(SMJ_flag){
       if(params_.debug && depth == 0) std::cout << "SMJ" << std::endl;
       get_emulated_cost_SMJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_R/", "part_rel_S/", counter_R[i], counter_S[i], depth + 1);
    }else{
       if(params_.debug && depth == 0) std::cout << "GHJ" << std::endl;
        x+= ceil(counter_R[i]*1.0/left_entries_per_page);
        get_emulated_cost_GHJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_R/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1); 
        
    }
    //remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
        //remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;
    

}

void Emulator::get_emulated_cost_ApprMatrixDP(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    std::vector<uint32_t> idxes = std::vector<uint32_t> (params_.k, 0U);
    for(auto i = 0; i < params_.k; i++){
    idxes[i] = i;
    }
    get_emulated_cost_ApprMatrixDP(idxes, params_.B, params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}


void Emulator::get_emulated_cost_ApprMatrixDP(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){

    
    // only set filter condition of S for now in case of tpch Q12 query 
    uint32_t R_filter_condition = 0;
    uint32_t S_filter_condition = params_.tpch_q12_flag ? 1 : 0;

    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t num_passes_R = 0;
    num_passes_R = ceil(left_num_entries*1.0/step_size);
    // if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(left_num_entries/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(right_num_entries/right_entries_per_page)){
    //     if(depth != 0){
    //                probe_start = std::chrono::high_resolution_clock::now();
    //            get_emulated_cost_NBJ("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, left_num_entries, true);
    //                probe_end = std::chrono::high_resolution_clock::now();
    //                probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
    //     }else{
    //                probe_start = std::chrono::high_resolution_clock::now();
    //            get_emulated_cost_NBJ(left_file_name, right_file_name, left_num_entries, right_num_entries, true);
    //                probe_end = std::chrono::high_resolution_clock::now();
    //                probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
    //     }
    // return;
    // }
    std::unordered_map<std::string, uint16_t> partitioned_keys;
    std::unordered_set<std::string> in_memory_keys; 
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();

    uint32_t num_partitions_for_ApprMatrixDP;
    uint32_t origin_num_partitions = params_.num_partitions;

    double pre_io_duration = io_duration;
    bool turn_on_NBJ = false;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    std::pair<uint32_t, uint32_t> partition_num_result = get_partitioned_keys(keys, key_multiplicity, partitioned_keys, in_memory_keys, turn_on_NBJ, true);
    uint32_t num_pre_partitions = partition_num_result.first;
    uint32_t num_random_in_mem_partitions = partition_num_result.second;
    std::chrono::time_point<std::chrono::high_resolution_clock>  algo_end = std::chrono::high_resolution_clock::now();
    algo_duration += (std::chrono::duration_cast<std::chrono::microseconds>(algo_end - partition_start)).count();
    //std::cout << "matrix dp #partitions: " << num_pre_partitions << std::endl;
  
    uint32_t num_remaining_entries = 0;
    uint32_t num_passes_left_entries = 0;

    num_remaining_entries = left_num_entries - in_memory_keys.size() - partitioned_keys.size();
    if (in_memory_keys.size() > 0) {
        params_.num_partitions -= 1;
        num_pre_partitions--;
    }

    if (turn_on_NBJ) {
        // no better results found than NBJ
	probe_start = std::chrono::high_resolution_clock::now();
        get_emulated_cost_NBJ(left_file_name, right_file_name, left_num_entries, right_num_entries, depth, R_filter_condition, S_filter_condition, true);
        probe_end = std::chrono::high_resolution_clock::now();
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count(); 
        return;
    }


    params_.num_partitions = params_.num_partitions - ceil(in_memory_keys.size()*params_.left_E_size*FUDGE_FACTOR/DB_PAGE_SIZE) - get_hash_map_size(in_memory_keys.size(), params_.join_key_size, 0) 
     - get_hash_map_size(partitioned_keys.size(), params_.join_key_size, 0);
    uint32_t tmp_num_random_in_mem_partitions = 0;
    uint32_t tmp_num_of_random_in_memory_entries = 0;
    params_.num_partitions = min(params_.num_partitions, 
      Emulator::est_best_num_partitions(tmp_num_random_in_mem_partitions, tmp_num_of_random_in_memory_entries, (FUDGE_FACTOR*params_.left_E_size)*1.0/DB_PAGE_SIZE, params_.num_partitions, num_remaining_entries*params_.left_selection_ratio, params_.hashtable_fulfilling_percent));

    num_passes_left_entries = ceil(num_remaining_entries*params_.left_selection_ratio/(step_size*params_.hashtable_fulfilling_percent));
     
    if(num_passes_left_entries + num_pre_partitions < params_.num_partitions && !params_.hybrid && num_random_in_mem_partitions == 0){ 
        // do not decrease the partition number when we expect there are some random partitions stay in memory
        std::cout << "The number of partitions automatically decreases to " << num_passes_left_entries + num_pre_partitions << " due to sufficient memory budget."<< std::endl;
        params_.num_partitions = num_passes_left_entries + num_pre_partitions;
    } else {
        std::cout << "#partitions are configured as " << params_.num_partitions << std::endl;
    }
    if (in_memory_keys.size()) {
        std::cout << in_memory_keys.size() << " entries are prioritized to be cached in-memory (not-spilled to disk) in hybrid hash join" << "."<< std::endl;
    }

    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);


    std::unordered_map<std::string, std::string> key2Rvalue;

    if(rounded_hash){
        double tmp_floated_num_passes = num_remaining_entries*params_.left_selection_ratio/(params_.num_partitions - num_pre_partitions)/step_size;
        if(params_.num_partitions > num_pre_partitions){
            if(tmp_floated_num_passes > 2 + params_.randwrite_seqread_ratio){
                num_passes_left_entries = 0;
            }else if(tmp_floated_num_passes > 2 + params_.seqwrite_seqread_ratio){
                num_passes_left_entries = ceil(num_remaining_entries/((2+params_.seqwrite_seqread_ratio)*(step_size*params_.hashtable_fulfilling_percent)));
            } 
        }
        if(ceil(tmp_floated_num_passes/params_.hashtable_fulfilling_percent) - ceil(tmp_floated_num_passes) >= 1){ //  do not use round hash if the filling percentage has go beyond the threshold
            num_passes_left_entries = 0;
        }

        partition_file(counter_R, partitioned_keys, in_memory_keys, key2Rvalue, num_pre_partitions, tmp_num_random_in_mem_partitions, left_file_name, params_.left_E_size, params_.left_table_size, num_passes_left_entries, params_.left_selection_ratio, &left_selection_seed, "part_rel_R/R", depth, R_filter_condition, true);
        partition_file(counter_S, partitioned_keys, in_memory_keys, key2Rvalue, num_pre_partitions, tmp_num_random_in_mem_partitions, right_file_name, params_.right_E_size, params_.right_table_size, num_passes_left_entries, params_.right_selection_ratio, &right_selection_seed, "part_rel_S/S", depth, S_filter_condition, false);
    }else{
        partition_file(counter_R, partitioned_keys, in_memory_keys, key2Rvalue, num_pre_partitions, tmp_num_random_in_mem_partitions, left_file_name, params_.left_E_size, params_.left_table_size, 0, params_.left_selection_ratio, &left_selection_seed, "part_rel_R/R", depth, R_filter_condition, true);
        partition_file(counter_S, partitioned_keys, in_memory_keys, key2Rvalue, num_pre_partitions, tmp_num_random_in_mem_partitions, right_file_name, params_.right_E_size, params_.right_table_size, 0, params_.right_selection_ratio, &right_selection_seed, "part_rel_S/S", depth, S_filter_condition, false);
    }
    
    if (key2Rvalue.size() > 0) {
        std::cout << key2Rvalue.size() << " records are cached in total in the partition phase" << std::endl;
    }
    if(params_.debug && num_pre_partitions != 0){
        print_counter_histogram(partitioned_keys, key_multiplicity, keys, num_pre_partitions);        
    }

    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);

    num_partitions_for_ApprMatrixDP = params_.num_partitions; 
    params_.num_partitions = origin_num_partitions;
    
    //probing
    uint32_t x = 0;
    bool SMJ_flag = false;
    for(auto i = 0; i < num_partitions_for_ApprMatrixDP; i++){
    
        if(counter_R[i] == 0 || counter_S[i] == 0){
            continue;
        }
        
        num_passes_R = ceil(counter_R[i]*1.0/step_size);
        if(params_.debug && depth == 0){
            std::cout << "counter_R : " << counter_R[i] << " --- counter_S : " << counter_S[i] << std::endl;
        }
        SMJ_flag = (!params_.no_smj_partition_wise_join) && ((ceil(counter_S[i]*1.0/(right_entries_per_page*2*(params_.B - 1))) + ceil(counter_R[i]*1.0/(left_entries_per_page*2*(params_.B - 1)))) < (params_.B - 1));
        if((num_passes_R <= 2 + params_.seqwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.seqwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page)) || 
            ((num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page)) && !SMJ_flag)){
        
            if(params_.debug && depth == 0) std::cout << "NBJ" << std::endl; 
            probe_start = std::chrono::high_resolution_clock::now();
            get_emulated_cost_NBJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1, 0, 0, true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();

        }else if(SMJ_flag){
            if(params_.debug && depth == 0) std::cout << "SMJ" << std::endl; 
            get_emulated_cost_SMJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_R/", "part_rel_S/", counter_R[i], counter_S[i], depth + 1);
        }else{
            if(params_.debug && depth == 0) std::cout << "GHJ" << std::endl; 
             x+= ceil(counter_R[i]*1.0/left_entries_per_page);
             get_emulated_cost_GHJ("part_rel_R/R-part-" + std::to_string(depth) + "-" + std::to_string(i), "part_rel_S/S-part-" + std::to_string(depth) + "-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1); 
        
         }
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;

}

template void Emulator::internal_sort<std::greater<std::pair<std::string, std::string>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_entries, double selection_ratio, uint64_t* selection_seed, uint32_t filter_condition, std::vector<uint32_t> & num_entries_per_run);
template void Emulator::internal_sort<std::less<std::pair<std::string, std::string>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_entries, double selection_ratio, uint64_t* selection_seed, uint32_t filter_condition, std::vector<uint32_t> & num_entries_per_run);
template void Emulator::merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no, std::vector<uint32_t> & num_entries_per_run);
template void Emulator::merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no, std::vector<uint32_t> & num_entries_per_run);
template void Emulator::merge_join<std::greater<std::pair<std::string, uint32_t>>>(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint8_t left_pass_no, uint8_t right_pass_no, std::vector<uint32_t> left_num_entries_per_run, std::vector<uint32_t> right_num_entries_per_run);
template void Emulator::merge_join<std::less<std::pair<std::string, uint32_t>>>(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint8_t left_pass_no, uint8_t right_pass_no, std::vector<uint32_t> left_num_entries_per_run, std::vector<uint32_t> right_num_entries_per_run);
