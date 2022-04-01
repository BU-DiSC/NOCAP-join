#include "parameters.h"
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
    left_entries_per_page = floor(DB_PAGE_SIZE/params_.left_E_size);
    right_entries_per_page = floor(DB_PAGE_SIZE/params_.right_E_size);

    join_entry_size = params_.left_E_size + params_.right_E_size - params_.K;
    join_output_entries_per_page = DB_PAGE_SIZE/join_entry_size;

}

void Emulator::print_counter_histogram(const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::vector<uint32_t> & key_multiplicity, const std::vector<std::string> & keys, uint32_t num_partitions){
    std::cout << "size of list-partitioned keys: " << partitioned_keys.size() << std::endl;
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

uint64_t Emulator::get_hash_value(std::string & key, HashType & ht, uint32_t seed){
    uint64_t result = 0;
    switch(ht){
        case MD5:
            memcpy(&result, md5(key).c_str(), sizeof(result));
            break;
        case SHA2:{
            uint8_t hash[32];
            const uint8_t * a = (const uint8_t*)key.c_str();
            calc_sha_256(hash, a, key.length());
            for(int i = 0; i < 32; i++){
               result = (result << 1) + (hash[i]&0x1);
            }
            break;
        }

        case MurMurhash: {
            result = MurmurHash64A( key.c_str(), key.size(), seed);
            break;
        }
        case RobinHood: {
	    result = robin_hood::hash_bytes(key.c_str(), key.size());
            break;
        }
        case XXhash:
        {
            // result = MurmurHash2(key.c_str(), key.size(), seed);
            XXH64_hash_t const p = seed;
            const void * key_void = key.c_str();
            XXH64_hash_t const h = XXH64(key_void, key.size(), p);
            result = h;
            break;
        }
        case CRC:{
            const void * key_void = key.c_str();
            result = crc32_fast( key_void, (unsigned long)key.size(), seed );
            break;
        }
        case CITY:{
            const char * key_void = key.c_str();
            result = CityHash64WithSeed( key_void, (unsigned long)key.size(), (unsigned long) seed);
            break;
        }
        default:
            result = MurmurHash2(key.c_str(), key.size(), seed);
            break;
    }

    return result;
}

inline uint32_t Emulator::get_hash_map_size(uint32_t k, uint32_t key_size, uint8_t size_of_partitionID){
    uint32_t dividend = k*FUDGE_FACTOR*(key_size + size_of_partitionID);
    if(dividend%DB_PAGE_SIZE == 0){
	return dividend/DB_PAGE_SIZE;
    }else{
	return dividend/DB_PAGE_SIZE + 1;
    }
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
        join_output_buffer = new char[DB_PAGE_SIZE];
        memset(join_output_buffer, 0, DB_PAGE_SIZE);
	opened = true;


        mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
        int write_flags = O_RDWR | O_TRUNC | O_CREAT;
        join_output_fd = open(params_.output_path.c_str(),write_flags,write_mode); 
    }

    output_cnt++;
    memcpy(join_output_buffer + join_output_offset, src1, len1);
    join_output_offset += len1;
    memcpy(join_output_buffer + join_output_offset, src2, len2);
    join_output_offset += len2;
    if(join_output_offset + (len1 + len2) > DB_PAGE_SIZE){
	write_and_clear_one_page(join_output_fd, join_output_buffer);
	write_cnt--; // does not count write for join output
	join_output_offset = 0;
    }
}

inline void Emulator::finish(){
    
    if(join_output_offset > 0){
	write_and_clear_one_page(join_output_fd, join_output_buffer);
	//memset(join_output_buffer, 0, DB_PAGE_SIZE);
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
    tmp_duration += (std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start)).count();
    read_cnt++;
    return read_bytes;
}


void Emulator::load_key_multiplicity(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, bool partial){
    std::ifstream fp;
    fp.open(params_.workload_dis_path.c_str(), std::ios::in);
    //std::cout << "Using " << params.workload_path << std::endl;
    fp >> params_.left_table_size >> params_.right_table_size >> params_.K >> params_.left_E_size >> params_.right_E_size;
    if(partial){
	keys = std::vector<std::string> (params_.k, "");
        key_multiplicity = std::vector<uint32_t> (params_.k, 0);
        for(auto i = 0; i < params_.k; i++){
	    fp >> keys[i] >> key_multiplicity[i];
        }
    }else{
	keys = std::vector<std::string> (params_.left_table_size, "");
        key_multiplicity = std::vector<uint32_t> (params_.left_table_size, 0);
        for(auto i = 0; i < params_.left_table_size; i++){
	    fp >> keys[i] >> key_multiplicity[i];
        }
    } 
    
    
    if(params_.c == 0){
	params_.c = log(log(params_.left_table_size)) + 2*log(params_.B) - log(log(params_.B));
    }
    if(params_.th == -1){
	params_.th = pow(static_cast<double>(params_.left_table_size) ,static_cast<double>(params_.B*params_.B)*1.0/params_.left_table_size)/(params_.B*1.0);
	if(params_.th > 1) params_.th = 1.0;
    }
    fp.close();
}

void Emulator::get_emulated_cost(){
    switch(params_.pjm){
        case BNLJ:
	    get_emulated_cost_BNLJ();
	    break;
	case Hash:
	    get_emulated_cost_HP();
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
}

void Emulator::get_emulated_cost_BNLJ(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_BNLJ(params_.workload_rel_R_path, params_.workload_rel_S_path, true);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
    probe_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}

void Emulator::get_emulated_cost_BNLJ(std::string left_file_name, std::string right_file_name, bool hash){
    uint32_t left_value_size = params_.left_E_size - params_.K;
    uint32_t right_value_size = params_.right_E_size - params_.K;
    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    char* R_rel_buffer = nullptr;
    char* S_rel_buffer = nullptr;
    int fd_R = open(left_file_name.c_str(), read_flags,read_mode ); 
    int fd_S = open(right_file_name.c_str(), read_flags,read_mode ); 

    bool end_flag_R = false;
    bool end_flag_S = false;
    uint32_t R_num_pages_in_buff = 0;
    ssize_t read_bytes_R = 0;
    ssize_t read_bytes_S = 0;
    size_t S_remainder = 0;
    size_t R_remainder = 0;
    int cmp_result = 0;
    char* src1_addr = nullptr;
    char* src1_end_addr = nullptr;
    char* src2_addr = nullptr;
    char* src2_end_addr = nullptr;

    char* tmp_buffer;
    std::string tmp_key;
    std::string tmp_value;
    std::unordered_map<std::string, std::string> key2Rvalue;
    
    posix_memalign((void**)&R_rel_buffer,DB_PAGE_SIZE,(params_.B - 1 - params_.BNLJ_inner_rel_buffer)*DB_PAGE_SIZE);
    posix_memalign((void**)&S_rel_buffer,DB_PAGE_SIZE,params_.BNLJ_inner_rel_buffer*DB_PAGE_SIZE);

    std::chrono::time_point<std::chrono::high_resolution_clock>  io_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  io_end;
    while(true){
       
        //auto tmp_start = std::chrono::high_resolution_clock::now();
        src2_end_addr = R_rel_buffer;
	R_num_pages_in_buff = 0;
	memset(R_rel_buffer, 0, (params_.B - 1 - params_.BNLJ_inner_rel_buffer)*DB_PAGE_SIZE);
	
        for(auto i = 0; i < params_.B - 1 - params_.BNLJ_inner_rel_buffer; i++){
	        read_bytes_R = read_one_page(fd_R, R_rel_buffer + i*DB_PAGE_SIZE);
	        if(read_bytes_R <= 0){
	    	    end_flag_R = true;
		    break;
	        }
	        R_num_pages_in_buff++;
	        src2_end_addr += DB_PAGE_SIZE;
        }

	if(R_rel_buffer[0] == '\0' && end_flag_R) break;

	
	if(hash){
	    key2Rvalue.clear(); 
	    for(auto i = 0; i < params_.B - 1 - params_.BNLJ_inner_rel_buffer && i < R_num_pages_in_buff ; i++){
		tmp_buffer = R_rel_buffer + i*DB_PAGE_SIZE;
		for(auto j = 0; j < left_entries_per_page; j++){
		    if(*(tmp_buffer+j*params_.left_E_size) == '\0') break;
		    tmp_key = std::string(tmp_buffer + j*params_.left_E_size, params_.K); 
		    tmp_value = std::string(tmp_buffer + j*params_.left_E_size + params_.K, left_value_size);
		    key2Rvalue[tmp_key] = tmp_value;
		}
	    }
	}

	lseek(fd_S, 0, SEEK_SET);
	end_flag_S = false; 
	uint32_t k = 0;


        while(true){
	    memset(S_rel_buffer, 0, params_.BNLJ_inner_rel_buffer*DB_PAGE_SIZE);
	    src1_end_addr = S_rel_buffer;
            for(auto i = 0; i < params_.BNLJ_inner_rel_buffer; i++){
    	        read_bytes_S = read_one_page(fd_S, S_rel_buffer + i*DB_PAGE_SIZE);
		src1_end_addr += DB_PAGE_SIZE;
	        if(read_bytes_S <= 0){
	            end_flag_S = true; 
		    break;
	        }
            }

	    // start nested loop join
	    src1_addr = S_rel_buffer;
	    while(*(src1_addr) != '\0' && src1_end_addr - src1_addr > 0 ){
		if(hash){
                    tmp_key = std::string(src1_addr, params_.K); 
		    if(key2Rvalue.find(tmp_key) != key2Rvalue.end()){
			add_one_record_into_join_result_buffer(src1_addr, params_.right_E_size, key2Rvalue[tmp_key].c_str(), left_value_size);
		    }

		}else{
                    src2_addr = R_rel_buffer;
		    while(*(src2_addr) != '\0' && src2_end_addr - src2_addr > 0){
                        cmp_result = memcmp(src1_addr, src2_addr, params_.K);
			
		        if(cmp_result == 0){
			    add_one_record_into_join_result_buffer(src1_addr, params_.right_E_size, src2_addr + params_.K, left_value_size);
		        }
		        src2_addr += params_.left_E_size;
		        R_remainder = (int)(src2_addr - R_rel_buffer)%DB_PAGE_SIZE;
		        if(DB_PAGE_SIZE - R_remainder < params_.left_E_size){
			    src2_addr += DB_PAGE_SIZE - R_remainder;
		        }
		    }
		}
	        
		src1_addr += params_.right_E_size;
		S_remainder = (int)(src1_addr - S_rel_buffer)%DB_PAGE_SIZE;
		if(DB_PAGE_SIZE - R_remainder < params_.right_E_size){
		    src1_addr += DB_PAGE_SIZE - S_remainder;
		}
	    }
	    if(end_flag_S) break;
	     
	}
	if(end_flag_R) break;


    }

    memset(S_rel_buffer, 0, DB_PAGE_SIZE*params_.BNLJ_inner_rel_buffer);
    memset(R_rel_buffer, 0, DB_PAGE_SIZE*(params_.B - 1 - params_.BNLJ_inner_rel_buffer));
    free(R_rel_buffer);
    free(S_rel_buffer);
    close(fd_R);
    close(fd_S);
}


template <typename T> uint32_t Emulator::internal_sort(std::string file_name, std::string prefix, uint32_t entry_size){
    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    uint32_t value_size = entry_size - params_.K;
    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    char* output_buffer;
    uint16_t output_records_counter_for_one_page = 0;
    uint16_t input_records_counter_for_one_page = entries_per_page;
    output_buffer = new char[DB_PAGE_SIZE];
    memset(input_buffer, 0, DB_PAGE_SIZE);
    memset(output_buffer, 0, DB_PAGE_SIZE);

    ssize_t read_bytes = 0;
    std::priority_queue<std::pair<std::string, std::string>, std::vector<std::pair<std::string, std::string> >, T  > pq1;
    std::priority_queue<std::pair<std::string, std::string>, std::vector<std::pair<std::string, std::string> >, T > pq2;
    int fd = open(file_name.c_str(), read_flags, read_mode);
    uint32_t read_pages = 0;
    char* tmp_offset = 0;
    while(read_pages <= params_.B - 3){
	read_bytes = read_one_page(fd, input_buffer);
	if(read_bytes <= 0) break;
	for(auto i = 0; i < entries_per_page; i++){
	    tmp_offset = input_buffer + i*entry_size;
	    pq1.emplace(std::string(tmp_offset, params_.K), std::string(tmp_offset + params_.K, value_size));
	}
	read_pages++;
    }

    uint32_t run_number = 0;
    std::string tmp_file_name = prefix + file_name + "-p0-run" + std::to_string(run_number);
    uint32_t output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
    std::string tmp_key1;
    std::string tmp_key2;
    while(true){
	
	if(!pq1.empty()){
	    tmp_key1 = pq1.top().first;
	    tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;
	    memcpy(tmp_offset, tmp_key1.c_str(), params_.K);
	    memcpy(tmp_offset + params_.K, pq1.top().second.c_str(), value_size);
	    output_records_counter_for_one_page++;
	    if(output_records_counter_for_one_page == entries_per_page){
	        write_and_clear_one_page(output_fd, output_buffer);
	        output_records_counter_for_one_page = 0U;
            }

	    pq1.pop();
	}
	
	if(pq1.empty()){
	    pq1.swap(pq2);

            if(output_records_counter_for_one_page > 0){
	        write_and_clear_one_page(output_fd, output_buffer);
	        output_records_counter_for_one_page = 0U;
	    }
	    fsync(output_fd);
	    close(output_fd);
	    run_number++;
            tmp_file_name = prefix + file_name + "-p0-run" + std::to_string(run_number);
	    
            output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
	}

	if(input_records_counter_for_one_page == entries_per_page){
	    input_records_counter_for_one_page = 0;
            memset(input_buffer, 0 ,DB_PAGE_SIZE);
            read_bytes = read_one_page(fd, input_buffer);
	    if(read_bytes <= 0) break;
	} 
	tmp_offset = input_buffer + input_records_counter_for_one_page*entry_size;
	if(*(tmp_offset) == '\0') break;
        input_records_counter_for_one_page++;
	tmp_key2 = std::string(tmp_offset, params_.K);
	if(tmp_key2 >= tmp_key1){
	    pq1.emplace(tmp_key2, std::string(tmp_offset + params_.K, value_size));
	}else{
	    pq2.emplace(tmp_key2, std::string(tmp_offset + params_.K, value_size));
	}
	   
	
    }
    close(fd);
    while(!pq1.empty()){
	tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;
	memcpy(tmp_offset, pq1.top().first.c_str(), params_.K);
	memcpy(tmp_offset + params_.K, pq1.top().second.c_str(), value_size);
	output_records_counter_for_one_page++;
	if(output_records_counter_for_one_page == entries_per_page){
	    write_and_clear_one_page(output_fd, output_buffer);
	    output_records_counter_for_one_page = 0U;
	}
	pq1.pop();
    }

    if(output_records_counter_for_one_page > 0){
	write_and_clear_one_page(output_fd, output_buffer);
	output_records_counter_for_one_page = 0U;
    }
    fsync(output_fd);
    close(output_fd);
    if(pq2.empty()) return run_number;

    run_number++;
    tmp_file_name = prefix + file_name + "-p0-run" + std::to_string(run_number);
    output_fd = open(tmp_file_name.c_str(), write_flags, write_mode);
    while(!pq2.empty()){
	tmp_offset = output_buffer + output_records_counter_for_one_page*entry_size;
	memcpy(tmp_offset, pq2.top().first.c_str(), params_.K);
	memcpy(tmp_offset + params_.K, pq2.top().second.c_str(), value_size);
	output_records_counter_for_one_page++;
	if(output_records_counter_for_one_page == entries_per_page){
	    write_and_clear_one_page(output_fd, output_buffer);
	    output_records_counter_for_one_page = 0U;
	}
	pq2.pop();
    }

    if(output_records_counter_for_one_page > 0){
	write_and_clear_one_page(output_fd, output_buffer);
	output_records_counter_for_one_page = 0U;
    }
    fsync(output_fd);
    close(output_fd);
    return run_number+1;

}

template <typename T> uint32_t Emulator::merge_sort_for_one_pass(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no){
    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;
    std::vector<int> fd_vec = std::vector<int> (params_.B - 1, -1);
    std::vector<uint16_t> input_records_for_one_page_vec = std::vector<uint16_t> (params_.B - 1, 0);

    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    uint16_t output_records_counter_for_one_page = 0;
    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,(params_.B-1)*DB_PAGE_SIZE);
    memset(input_buffer, 0, (params_.B-1)*DB_PAGE_SIZE);
    char* output_buffer;
    output_buffer = new char[DB_PAGE_SIZE];
    memset(output_buffer, 0, DB_PAGE_SIZE);

    uint32_t run_idx = 0;
    uint32_t run_inner_idx = 0;
    uint32_t tmp_run_idx = 0;
    std::string prefix_input_filename = prefix + file_name + "-p" + std::to_string(pass_no) + "-run";
    std::string prefix_output_filename = prefix + file_name + "-p" + std::to_string(pass_no+1) + "-run";
    std::string tmp_str;
    uint32_t new_run_num = 0;
    uint32_t read_bytes = 0;
    int output_fd;

    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T> pq;

    char* tmp_offset1;
    char* tmp_offset2;
    uint16_t tmp_input_records_for_one_page = 0;
    uint32_t top_run_idx;
    while(run_idx < num_runs){
	
        for(tmp_run_idx = run_idx; tmp_run_idx - run_idx < params_.B - 1 && tmp_run_idx < num_runs; tmp_run_idx++){
            run_inner_idx = tmp_run_idx - run_idx;
	    tmp_str = prefix_input_filename + std::to_string(tmp_run_idx);
	    fd_vec[run_inner_idx] = open(tmp_str.c_str(), read_flags, read_mode);
	    tmp_offset1 = input_buffer + run_inner_idx*DB_PAGE_SIZE;
            read_bytes = read_one_page(fd_vec[run_inner_idx], tmp_offset1);
	    if(read_bytes <= 0){
		close(fd_vec[run_inner_idx]);
		fd_vec[run_inner_idx] = -1;
		memset(tmp_offset1, 0, DB_PAGE_SIZE);
	    }
	    input_records_for_one_page_vec[run_inner_idx] = 0;
	    if(*tmp_offset1 != '\0'){
		pq.emplace(std::string(tmp_offset1, params_.K), run_inner_idx);
	    }
	}
	tmp_str = prefix_output_filename + std::to_string(new_run_num);
        output_fd = open(tmp_str.c_str(), write_flags, write_mode);

	while(!pq.empty()){
	    tmp_offset1 = output_buffer + output_records_counter_for_one_page*entry_size;
	    top_run_idx = pq.top().second;
	    
	    pq.pop();
	    tmp_offset2 = input_buffer + top_run_idx*DB_PAGE_SIZE + input_records_for_one_page_vec[top_run_idx]*entry_size;
	    memcpy(tmp_offset1, tmp_offset2, entry_size);
            output_records_counter_for_one_page++;
	    if(output_records_counter_for_one_page == entries_per_page){
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
		}
                
		
	    }else{
		tmp_offset2 += entry_size;
	    }

	    if(*tmp_offset2 != '\0'){
		tmp_str = std::string(tmp_offset2, params_.K);
		pq.emplace(tmp_str.c_str(), top_run_idx);
	    }


	}

        if(output_records_counter_for_one_page > 0){
	    write_and_clear_one_page(output_fd, output_buffer);
	    output_records_counter_for_one_page = 0U;
	}
	fsync(output_fd);
	close(output_fd);

	run_idx = tmp_run_idx;
	new_run_num++;
	
    }
    return new_run_num;

}


template <typename T> void Emulator::merge_join(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint32_t left_num_runs, uint32_t right_num_runs, uint8_t left_pass_no, uint8_t right_pass_no){
    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    std::vector<int> left_fd_vec = std::vector<int> (left_num_runs, -1);
    std::vector<uint16_t> left_input_records_for_one_page_vec = std::vector<uint16_t> (left_num_runs, 0);
    uint32_t left_entries_per_page = DB_PAGE_SIZE/left_entry_size;
    uint32_t left_value_size = left_entry_size - params_.K;
    char* left_input_buffer;
    posix_memalign((void**)&left_input_buffer,DB_PAGE_SIZE,left_num_runs*DB_PAGE_SIZE);
    memset(left_input_buffer, 0, left_num_runs*DB_PAGE_SIZE);
    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T > left_pq;

    std::vector<int> right_fd_vec = std::vector<int> (right_num_runs, -1);
    std::vector<uint16_t> right_input_records_for_one_page_vec = std::vector<uint16_t> (right_num_runs, 0);
    uint32_t right_entries_per_page = DB_PAGE_SIZE/right_entry_size;
    char* right_input_buffer;
    posix_memalign((void**)&right_input_buffer,DB_PAGE_SIZE,right_num_runs*DB_PAGE_SIZE);
    memset(right_input_buffer, 0, right_num_runs*DB_PAGE_SIZE);
    std::priority_queue<std::pair<std::string, uint32_t>, std::vector<std::pair<std::string, uint32_t> >, T > right_pq;



    // load the first pages
    std::string tmp_str;
    char* tmp_offset;
    uint32_t read_bytes = 0;
    std::string prefix_input_filename = left_file_prefix + "-p" + std::to_string(left_pass_no) + "-run";
    for(uint32_t i = 0; i < left_num_runs; i++){
	tmp_str = prefix_input_filename + std::to_string(i);
        left_fd_vec[i] = open(tmp_str.c_str(), read_flags, read_mode);
	tmp_offset = left_input_buffer + i*DB_PAGE_SIZE;
        read_bytes = read_one_page(left_fd_vec[i], tmp_offset);
        if(read_bytes <= 0){
	    left_fd_vec[i] = -1;
	    memset(tmp_offset, 0, DB_PAGE_SIZE);
	}
        if(*tmp_offset != '\0'){
	    left_pq.emplace(std::string(tmp_offset, params_.K), i);
	}
    }

    prefix_input_filename = right_file_prefix + "-p" + std::to_string(right_pass_no) + "-run";
    for(uint32_t i = 0; i < right_num_runs; i++){
	tmp_str = prefix_input_filename + std::to_string(i);
        right_fd_vec[i] = open(tmp_str.c_str(), read_flags, read_mode);
	tmp_offset = right_input_buffer + i*DB_PAGE_SIZE;
        read_bytes = read_one_page(right_fd_vec[i], tmp_offset);
        if(read_bytes <= 0){
	    right_fd_vec[i] = -1;
	    memset(tmp_offset, 0, DB_PAGE_SIZE);
	}
        if(*tmp_offset != '\0'){
	    right_pq.emplace(std::string(tmp_offset, params_.K), i);
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
	    add_one_record_into_join_result_buffer(right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE + right_input_records_for_one_page_vec[right_tmp_run_idx]*right_entry_size, right_entry_size, tmp_offset + params_.K, left_value_size);
	}

	if(matched_result == 0 || (params_.SMJ_greater_flag && matched_result > 0) || (!params_.SMJ_greater_flag && matched_result < 0) ){
	    right_pq.pop();
	    right_input_records_for_one_page_vec[right_tmp_run_idx]++;
	    if(right_input_records_for_one_page_vec[right_tmp_run_idx] == right_entries_per_page){
                right_input_records_for_one_page_vec[right_tmp_run_idx] = 0;
		tmp_offset2 = right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE;
		memset(tmp_offset2, 0, DB_PAGE_SIZE);
                read_bytes = read_one_page(right_fd_vec[right_tmp_run_idx], tmp_offset2);
	        if(read_bytes <= 0){
		    memset(tmp_offset2, 0, DB_PAGE_SIZE);
		    close(right_fd_vec[right_tmp_run_idx]);
		    right_fd_vec[right_tmp_run_idx] = -1;
		}
	    }else{
	        tmp_offset2 = right_input_buffer + right_tmp_run_idx*DB_PAGE_SIZE + right_input_records_for_one_page_vec[right_tmp_run_idx]*right_entry_size;
	    }

	    if(*tmp_offset2 != '\0'){
		right_pq.emplace(std::string(tmp_offset2, params_.K), right_tmp_run_idx);
	    }
	}else{
	    left_pq.pop();
	    left_input_records_for_one_page_vec[left_tmp_run_idx]++;
	    if(left_input_records_for_one_page_vec[left_tmp_run_idx] == left_entries_per_page){
                left_input_records_for_one_page_vec[left_tmp_run_idx] = 0;
		tmp_offset2 = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE;
		memset(tmp_offset2, 0, DB_PAGE_SIZE);
                read_bytes = read_one_page(left_fd_vec[left_tmp_run_idx], tmp_offset2);
	        if(read_bytes <= 0){
		    memset(tmp_offset2, 0, DB_PAGE_SIZE);
		    close(left_fd_vec[left_tmp_run_idx]);
		    left_fd_vec[left_tmp_run_idx] = -1;
		}
	    }else{
	        tmp_offset2 = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE + left_input_records_for_one_page_vec[left_tmp_run_idx]*left_entry_size;
	    }

	    if(*tmp_offset2 != '\0'){
		left_pq.emplace(std::string(tmp_offset2, params_.K), left_tmp_run_idx);
	    }

	    if(!left_pq.empty()){
                tmp_str = left_pq.top().first;
	        left_tmp_run_idx = left_pq.top().second;
	        tmp_offset = left_input_buffer + left_tmp_run_idx*DB_PAGE_SIZE + left_input_records_for_one_page_vec[left_tmp_run_idx]*left_entry_size;
	    }
	}
        
    }
    return;

}


void Emulator::get_emulated_cost_SMJ(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_SMJ(params_.workload_rel_R_path, params_.workload_rel_S_path);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}


void Emulator::get_emulated_cost_SMJ(std::string left_file_name, std::string right_file_name){
    /*
    uint32_t num_passes_R = ceil(params_.left_table_size*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    if(num_passes_R < 3 || 2*ceil(params_.left_table_size/left_entries_per_page) >= (num_passes_R - 3)*ceil(params_.right_table_size/right_entries_per_page)){
        auto probe_start = std::chrono::high_resolution_clock::now();
	get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
        auto probe_end = std::chrono::high_resolution_clock::now();
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	return;
    }*/
    system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
    system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
    uint8_t left_num_pass = 0;
    uint8_t right_num_pass = 0;
    uint32_t num_R_runs, num_S_runs;
    if(params_.SMJ_greater_flag){
	num_R_runs = internal_sort<std::greater<std::pair<std::string, std::string>>>(left_file_name, "part_rel_R/", params_.left_E_size);
	std::cout << "num_R_runs : " << num_R_runs << std::endl;
        num_S_runs = internal_sort<std::greater<std::pair<std::string, std::string>>>(right_file_name, "part_rel_S/", params_.right_E_size);
	std::cout << "num_S_runs : " << num_S_runs << std::endl;
    
        while(num_R_runs + num_S_runs > params_.B - 1){
            if(num_R_runs < num_S_runs){
		if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1){
                    num_R_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
		    left_num_pass++;
		}else if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1 || num_R_runs == 1){
                    num_S_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    right_num_pass++;
		}else{
                    num_R_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
                    num_S_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    left_num_pass++;
		    right_num_pass++;
		}
	    }else{
                if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1){
                    num_S_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    right_num_pass++;
		}else if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1 || num_S_runs == 1){
                    num_R_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
		    left_num_pass++;
		}else{
                    num_R_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
                    num_S_runs = merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    left_num_pass++;
		    right_num_pass++;
		}
	    }
	
	    std::cout << "pass no: " << (int)left_num_pass << " done (#runs for R: " << num_R_runs << "), pass no: " << (int)right_num_pass << "  (#runs for S: " << num_S_runs << ")" << std::endl;
        }
        merge_join<std::greater<std::pair<std::string, uint32_t>>>("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, params_.left_E_size, params_.right_E_size, num_R_runs, num_S_runs, left_num_pass, right_num_pass); 
    }else{
	num_R_runs = internal_sort<std::less<std::pair<std::string, std::string>>>(left_file_name, "part_rel_R/", params_.left_E_size);
        num_S_runs = internal_sort<std::less<std::pair<std::string, std::string>>>(right_file_name, "part_rel_S/", params_.right_E_size);
    
        while(num_R_runs + num_S_runs > params_.B - 1){
           if(num_R_runs < num_S_runs){
		if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1){
                    num_R_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
		    left_num_pass++;
		}else if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1 || num_R_runs == 1){
                    num_S_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    right_num_pass++;
		}else{
                    num_R_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
                    num_S_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    left_num_pass++;
		    right_num_pass++;
		}
	    }else{
                if(ceil(num_S_runs/(params_.B - 1)) + num_R_runs <= params_.B - 1){
                    num_S_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    right_num_pass++;
		}else if(ceil(num_R_runs/(params_.B - 1)) + num_S_runs <= params_.B - 1 || num_S_runs == 1){
                    num_R_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
		    left_num_pass++;
		}else{
                    num_R_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(left_file_name, "part_rel_R/", params_.left_E_size, num_R_runs, left_num_pass);
                    num_S_runs = merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(right_file_name, "part_rel_S/", params_.right_E_size, num_S_runs, right_num_pass); 
		    left_num_pass++;
		    right_num_pass++;
		}
	    }
	
	    std::cout << "pass no: " << (int)left_num_pass << " done (#runs for R: " << num_R_runs << "), pass no: " << (int)right_num_pass << "  (#runs for S: " << num_S_runs << ")" << std::endl;
        }
        merge_join<std::less<std::pair<std::string, uint32_t>>>("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, params_.left_E_size, params_.right_E_size, num_R_runs, num_S_runs, left_num_pass, right_num_pass); 
    }
    

}


void Emulator::partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint16_t> & partitioned_keys, const std::unordered_set<std::string> & top_matching_keys, uint32_t num_pre_partitions, std::string file_name, uint32_t entry_size, uint32_t divider, std::string prefix, uint32_t depth){
    bool prepartitioned = num_pre_partitions > 0;
    
    uint32_t entries_per_page = DB_PAGE_SIZE/entry_size;
    char* buffer;
    posix_memalign((void**)&buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    char* output_buffer;
    output_buffer = new char[params_.num_partitions*DB_PAGE_SIZE];
    memset(buffer, 0, DB_PAGE_SIZE);
    std::vector<uint32_t> offsets = std::vector<uint32_t> (params_.num_partitions, 0);
    memset(output_buffer, 0, params_.num_partitions*DB_PAGE_SIZE);
    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
    mode_t write_mode = S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH;

    uint64_t hash_value; 
    HashType tmp_ht = params_.ht;
    tmp_ht = static_cast<HashType> ((tmp_ht + depth)%6U);
    uint32_t subpartition_idx = 0;
    ssize_t read_bytes = 0;
    int fd;
    if(depth != 0){
        fd = open((prefix + file_name).c_str(), read_flags, read_mode);
    }else{
        fd = open(file_name.c_str(), read_flags, read_mode);
    }

    std::vector<int> fd_vec = std::vector<int> ( params_.num_partitions, -1);

    std::string tmp_str = "";
    while(true){
	read_bytes = read_one_page(fd, buffer);
	if(read_bytes <= 0) break;

	for(auto i = 0; i < entries_per_page; i++){
	    tmp_str = std::string(buffer + i*entry_size, params_.K);
	    if(!prepartitioned){
                hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
		if(divider == 0){
	            subpartition_idx = hash_value%(params_.num_partitions);
		}else{
	            subpartition_idx = (hash_value%(divider))%params_.num_partitions;
		    /*
		    if(subpartition_idx >= lb_partition_id_th){
			subpartition_idx = (subpartition_idx - lb_partition_id_th)%(params_.num_partitions - lb_partition_id_th) + lb_partition_id_th;
		    }*/
		}
	    }else{

		if((num_pre_partitions != 1 && partitioned_keys.find(tmp_str) == partitioned_keys.end()) || (num_pre_partitions == 1 && top_matching_keys.find(tmp_str) == top_matching_keys.end())){
		    if(divider != 0){
                        hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
                        subpartition_idx = (hash_value%(divider))%(params_.num_partitions - num_pre_partitions);
			/*
		        if(subpartition_idx >= lb_partition_id_th){
			    subpartition_idx = (subpartition_idx - lb_partition_id_th)%(params_.num_partitions - lb_partition_id_th - num_pre_partitions) + lb_partition_id_th;
		        }*/
			subpartition_idx += num_pre_partitions;
		    }else{
	                continue; // ignore keys if they are not in the given partition map
		    }
		}else if(num_pre_partitions != 1){
		    subpartition_idx = partitioned_keys.at(tmp_str);
		}else{
		    subpartition_idx = 0;
		}
	    }
	    memcpy(output_buffer + subpartition_idx*DB_PAGE_SIZE+offsets[subpartition_idx], buffer + i*entry_size, entry_size);
	    offsets[subpartition_idx] += entry_size;
	    if(DB_PAGE_SIZE < offsets[subpartition_idx] + entry_size){
		    if(fd_vec[subpartition_idx] == -1){
	                tmp_str = prefix + file_name + "-part-" + std::to_string(subpartition_idx);
	                fd_vec[subpartition_idx] = open(tmp_str.c_str(), write_flags, write_mode);
			//if(fd_vec[subpartition_idx] == -1) printf("Error: %s\n", strerror(errno)); 
	            }
		    write_and_clear_one_page(fd_vec[subpartition_idx], output_buffer + subpartition_idx*DB_PAGE_SIZE);
		    offsets[subpartition_idx] = 0;
	    }
	    counter[subpartition_idx]++;
	}
    }
    for(auto i = 0; i < params_.num_partitions; i++){
        if(offsets[i] != 0){
            if(fd_vec[i] == -1){
	        tmp_str = prefix + file_name + "-part-" + std::to_string(i);
	        fd_vec[i] = open(tmp_str.c_str(), write_flags, write_mode);
	    }
	    write_and_clear_one_page(fd_vec[i], output_buffer + i*DB_PAGE_SIZE);
	    fsync(fd_vec[i]);
	    close(fd_vec[i]);
	}else if(fd_vec[i] != -1){
	    fsync(fd_vec[i]);
	    close(fd_vec[i]);
	}
    }
    fd_vec.clear();
    close(fd);

}

void Emulator::get_emulated_cost_HP(){
    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    get_emulated_cost_HP(params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();

}


void Emulator::get_emulated_cost_HP(std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_um_entries, uint32_t depth){
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t num_passes_R = ceil(left_num_entries*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    if(num_passes_R < 2 + params_.randwrite_seqread_ratio || 2*ceil(params_.left_table_size/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*ceil(params_.right_table_size/right_entries_per_page)){
        probe_start = std::chrono::high_resolution_clock::now();
	get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
        probe_end = std::chrono::high_resolution_clock::now();
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	return;
    }
    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);

    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    if(depth == 0U){
        system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
        system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
    }
    if(rounded_hash){
	if(depth == 0) std::cout << "Num passes R: " << num_passes_R << std::endl;
        partition_file(counter_R, {}, {}, 0, left_file_name, params_.left_E_size, num_passes_R, "part_rel_R/", depth); 
        partition_file(counter_S, {}, {}, 0, right_file_name, params_.right_E_size, num_passes_R, "part_rel_S/", depth); 
    }else{
        partition_file(counter_R, {}, {}, 0, left_file_name, params_.left_E_size, 0, "part_rel_R/", depth); 
        partition_file(counter_S, {}, {}, 0, right_file_name, params_.right_E_size,0, "part_rel_S/", depth); 
    }
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);

    uint32_t x = 0;
    //probing
    for(auto i = 0; i < params_.num_partitions; i++){
	if(counter_R[i] == 0 || counter_S[i] == 0){
	    if(counter_R[i] != 0) 
	        remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    if(counter_S[i] != 0) 
	        remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	    continue;
	}
        /*
	probe_start = std::chrono::high_resolution_clock::now();
	get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(i), "part_rel_S/" + right_file_name + "-part-" + std::to_string(i), true);

        probe_end = std::chrono::high_resolution_clock::now();
	    
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());*/
	
	num_passes_R = ceil(counter_R[i]*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
	if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*ceil(counter_S[i]/right_entries_per_page)){
	//if(num_passes_R <= 1){
            probe_start = std::chrono::high_resolution_clock::now();
	    get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(i), "part_rel_S/" + right_file_name + "-part-" + std::to_string(i), true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	    remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	}else{

	   get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1);
	   /*
	   if(depth == 0){
               auto tmp_start = std::chrono::high_resolution_clock::now();
	        get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), depth + 1);
               auto tmp_end = std::chrono::high_resolution_clock::now();
               tmp_duration += (std::chrono::duration_cast<std::chrono::microseconds>(tmp_end - tmp_start)).count();
	   }else{
	       get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), depth + 1);
	   }*/
	    x+= ceil(counter_R[i]*1.0/left_entries_per_page);
	}
	
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
    uint32_t num_passes_R = ceil(params_.left_table_size*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    if(num_passes_R <= 2 + params_.randwrite_seqread_ratio){
	get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
	return;
    }

    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);

    std::vector<unsigned char> page_out_bits;
    if(params_.num_partitions%8 == 0){
	page_out_bits = std::vector<unsigned char>(params_.num_partitions >> 3, 0U);
    }else{
	page_out_bits = std::vector<unsigned char>((params_.num_partitions >> 3) +1, 0U);
    }
    uint16_t num_paged_out_partitions = 0U;
    uint16_t in_mem_pages = 0U;
    bool curr_page_out = false;

    HashType partition_ht = params_.ht;
    partition_ht = static_cast<HashType> ((partition_ht + depth)%6U);
    HashType probe_ht = static_cast<HashType> ((partition_ht + 1 + depth)%6U);

    std::chrono::time_point<std::chrono::high_resolution_clock>  io_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  io_end;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();

    char* input_buffer;
    posix_memalign((void**)&input_buffer,DB_PAGE_SIZE,DB_PAGE_SIZE);
    char* rest_buffer = new char[(params_.B - 2)*DB_PAGE_SIZE];
    memset(rest_buffer, 0, (params_.B - 2)*DB_PAGE_SIZE);

    std::vector<uint16_t> offsets = std::vector<uint16_t> (params_.num_partitions, 0);
    std::vector<std::deque<uint16_t> > partitioned_page_idxes = std::vector<std::deque<uint16_t> > (params_.num_partitions, std::deque<uint16_t>());
    std::vector<int> fd_vec = std::vector<int> ( params_.num_partitions, -1);


    int read_flags = O_RDONLY | O_DIRECT;
    mode_t read_mode = S_IRUSR | S_IRGRP | S_IROTH;
    int write_flags = O_RDWR | O_TRUNC | O_CREAT;
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
        system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
        system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
        fd_R = open(left_file_name.c_str(), read_flags, read_mode);
        fd_S = open(right_file_name.c_str(), read_flags, read_mode);
    }

    std::deque<uint16_t> avail_page_idxes;
    for(uint16_t i = 0; i < (uint16_t) (params_.num_partitions); i++){
	partitioned_page_idxes[i].push_back(i);
    }
    for(uint16_t i = (uint16_t) (params_.num_partitions); i < (uint16_t) (params_.B - 2); i++){
	avail_page_idxes.push_back(i);
    } 
    
    //partition R
    std::string tmp_str = "";
    std::string tmp_file_str = "";
    uint16_t page_id = 0U;
    while(true){
	read_bytes = read_one_page(fd_R, input_buffer);
	if(read_bytes <= 0) break;

	for(auto i = 0; i < left_entries_per_page; i++){
	    tmp_str = std::string(input_buffer + i*params_.left_E_size, params_.K);
            hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
	    subpartition_idx = hash_value%(params_.num_partitions);
	    page_id = partitioned_page_idxes[subpartition_idx].back();
	    memcpy(rest_buffer + page_id*DB_PAGE_SIZE+offsets[subpartition_idx], input_buffer + i*params_.left_E_size, params_.left_E_size);
	    offsets[subpartition_idx] += params_.left_E_size;
	    if(DB_PAGE_SIZE < offsets[subpartition_idx] + params_.left_E_size){
		curr_page_out = (page_out_bits[subpartition_idx/8] >> (subpartition_idx%8)) & 1U;
		if(!curr_page_out){ // if the partition has not been paged out, we look for a partition to page out and release pages
		    in_mem_pages = params_.B - 2 - num_paged_out_partitions;
                    //if(avail_page_idxes.size() == 0){// when we do not have available pages
		    if(left_entries_per_page*(in_mem_pages - avail_page_idxes.size()) > floor(left_entries_per_page*in_mem_pages/FUDGE_FACTOR)){ // when the in-memory hash table can still be held in the memory
			//std::cout << "avail page size: " << avail_page_idxes.size() << std::endl;
			uint16_t max_size_of_a_partition = 0U;
			uint16_t partition_idx_to_be_evicted = 0U;
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
	                        tmp_file_str = "part_rel_R/" + left_file_name + "-part-" + std::to_string(partition_idx_to_be_evicted);
	                        fd_vec[partition_idx_to_be_evicted] = open(tmp_file_str.c_str(), write_flags, write_mode);
			//if(fd_vec[subpartition_idx] == -1) printf("Error: %s\n", strerror(errno)); 
	                    }
			    uint16_t page_id_to_be_evicted = 0U;
			    for(auto i = 0; i < max_size_of_a_partition - 1; i++){
			        page_id_to_be_evicted = partitioned_page_idxes[partition_idx_to_be_evicted].front();
				partitioned_page_idxes[partition_idx_to_be_evicted].pop_front();
				write_and_clear_one_page(fd_vec[partition_idx_to_be_evicted], rest_buffer + page_id_to_be_evicted*DB_PAGE_SIZE);
			        avail_page_idxes.push_back(page_id_to_be_evicted);
			    } 
                            page_out_bits[partition_idx_to_be_evicted/8] = page_out_bits[partition_idx_to_be_evicted/8] | (1 << (partition_idx_to_be_evicted%8)); // set the page-out bit
			    if(partition_idx_to_be_evicted == subpartition_idx) curr_page_out = true;
			    //std::cout << "paging out patition ID: " << partition_idx_to_be_evicted << " \t available pages: " << avail_page_idxes.size() << std::endl;
	                    //counter_R[partition_idx_to_be_evicted] = 0;
			}else{ // flush the current partition (will be executed later)
                            page_out_bits[subpartition_idx/8] = page_out_bits[subpartition_idx/8] | (1 << (subpartition_idx%8)); // set the page-out bit
			    curr_page_out = true;
	                    //counter_R[subpartition_idx] = 0;
			}
		    }
		}


		if(curr_page_out){
		    if(fd_vec[subpartition_idx] == -1){
	                tmp_file_str = "part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx);
	                fd_vec[subpartition_idx] = open(tmp_file_str.c_str(), write_flags, write_mode);
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

    std::unordered_map<std::string, std::string> key2Rvalue;
    // we maintain this memory-resident memory here, which means that the hash table may co-exist with the pages we newed above. We simplify this implementation but this may not occur in the pratical system since there usually exists a global buffer manager that can recylce pages at any time.

    //flush partitions if they are marked paged-out amd build the hash table
    key2Rvalue.clear(); 
    char* tmp_buffer;
    std::unordered_map<uint16_t, uint16_t> partitionId2outbufferId;
    uint16_t outbufferId = 0U;
    for(auto i = 0; i < params_.num_partitions; i++){
	if((page_out_bits[i/8] >> (i%8)) & 1U){
	    partitionId2outbufferId[i] = outbufferId;
	    outbufferId++;
	    if(offsets[i] != 0){
                if(fd_vec[i] == -1){
	            tmp_file_str = "part_rel_R/" + left_file_name + "-part-" + std::to_string(i);
	            fd_vec[i] = open(tmp_file_str.c_str(), write_flags, write_mode);
	        }
		write_and_clear_one_page(fd_vec[i], rest_buffer + partitioned_page_idxes[i][0]*DB_PAGE_SIZE);
		fsync(fd_vec[i]);
	        close(fd_vec[i]);
	    }else if(fd_vec[i] != -1){
		fsync(fd_vec[i]);
	        close(fd_vec[i]);
	    }
	}else{
	    for(auto t_page_idx:partitioned_page_idxes[i]){
                tmp_buffer = rest_buffer + t_page_idx*DB_PAGE_SIZE;
		for(auto j = 0; j < left_entries_per_page; j++){
		    if(*(tmp_buffer+j*params_.left_E_size) == '\0') break;
		    key2Rvalue[std::string(tmp_buffer + j*params_.left_E_size, params_.K)] = std::string(tmp_buffer + j*params_.left_E_size + params_.K, params_.left_E_size - params_.K);
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
    rest_buffer = new char[num_paged_out_partitions*DB_PAGE_SIZE]; 
    memset(rest_buffer, 0, num_paged_out_partitions*DB_PAGE_SIZE);
    offsets = std::vector<uint16_t> (num_paged_out_partitions, 0);
    fd_vec = std::vector<int> (num_paged_out_partitions, -1);
    std::cout << "#paged-out partitions: " << num_paged_out_partitions << std::endl; 
    //partition S 
    while(true){
	read_bytes = read_one_page(fd_S, input_buffer);
	if(read_bytes <= 0) break;

	for(auto j = 0; j < right_entries_per_page; j++){
	    tmp_str = std::string(input_buffer + j*params_.right_E_size, params_.K);
            hash_value = get_hash_value(tmp_str, tmp_ht, s_seed);
	    subpartition_idx = hash_value%(params_.num_partitions);
	    if(partitionId2outbufferId.find(subpartition_idx) == partitionId2outbufferId.end()){ // if it does not find the entry in paged-out partition set
		if(key2Rvalue.find(tmp_str) != key2Rvalue.end()){ // write to the join output buffer if it matches
	            add_one_record_into_join_result_buffer(input_buffer + j*params_.right_E_size, params_.right_E_size, key2Rvalue[tmp_str].c_str(), params_.left_E_size - params_.K);
		}
	    }else{ // write to the buffer which is going to be flushed when it is full
		 page_id = partitionId2outbufferId[subpartition_idx];

                 if(fd_vec[page_id] == -1){
	             tmp_file_str = "part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx);
	             fd_vec[page_id] = open(tmp_file_str.c_str(), write_flags, write_mode);
	         }
		 memcpy(rest_buffer + page_id*DB_PAGE_SIZE + offsets[page_id], input_buffer + j*params_.right_E_size, params_.right_E_size);
		 offsets[page_id] += params_.right_E_size;

		 if(DB_PAGE_SIZE < offsets[page_id] + params_.right_E_size){
                     write_and_clear_one_page(fd_vec[page_id], rest_buffer + page_id*DB_PAGE_SIZE);
		     offsets[page_id] = 0;
		 }
	    }
	    counter_S[subpartition_idx]++;
	}
    }
    close(fd_S);

    for(auto subpartition_idx_iter = partitionId2outbufferId.begin(); subpartition_idx_iter !=  partitionId2outbufferId.end(); subpartition_idx_iter++){
	page_id = subpartition_idx_iter->second;
	if(offsets[page_id] != 0){
	    if(fd_vec[page_id] == -1){
	             tmp_file_str = "part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx_iter->first);
	             fd_vec[page_id] = open(tmp_file_str.c_str(), write_flags, write_mode);
	    }
	    write_and_clear_one_page(fd_vec[page_id], rest_buffer + page_id*DB_PAGE_SIZE);
	    offsets[page_id] = 0;
	    close(fd_vec[page_id]);
	}else if(fd_vec[page_id] != -1){
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
	    if(counter_R[subpartition_idx] != 0) 
	        remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
	    if(counter_S[subpartition_idx] != 0) 
	        remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
	    continue;
	}

	/*
        probe_start = std::chrono::high_resolution_clock::now();
	get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx), "part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx), true);

        probe_end = std::chrono::high_resolution_clock::now();
	    
        probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
	remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());*/

	
	num_passes_R = ceil(counter_R[subpartition_idx]*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
	if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[subpartition_idx]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[subpartition_idx]/right_entries_per_page)){
	//if(num_passes_R <= 1){
	    
            probe_start = std::chrono::high_resolution_clock::now();
	    get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx), "part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx), depth + 1);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	    remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
	    remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(subpartition_idx)).c_str());
	}else{
	    x+= ceil(counter_R[subpartition_idx]*1.0/left_entries_per_page);

	    get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(subpartition_idx), right_file_name + "-part-" + std::to_string(subpartition_idx), counter_R[subpartition_idx], counter_S[subpartition_idx], depth + 1);
	   
	}
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;
}


uint32_t Emulator::get_partitioned_keys(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::unordered_map<std::string, uint16_t> & partitioned_keys, std::unordered_set<std::string> & top_matching_keys, Params & params, bool appr_flag){
    partitioned_keys.clear();
    top_matching_keys.clear();
    
    uint32_t left_entries_per_page = params.page_size/params.left_E_size;
    uint32_t step_size = (uint32_t) floor(left_entries_per_page*(params.B - 1 - params.BNLJ_inner_rel_buffer)/FUDGE_FACTOR);
    std::cout << "step size: " << step_size << std::endl;

    uint32_t hash_map_size = 0;
    uint32_t m_r = 0;
    uint32_t num_remaining_keys;
    uint32_t num_remaining_matches;

    uint64_t summed_match = 0;
    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;
    for(uint32_t i = 0; i < keys.size(); i++){
	if(key_multiplicity[i] == 0) continue;
	summed_match += key_multiplicity[i];
	key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity[i], i));
    }
    std::reverse(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    uint32_t n = key_multiplicity_to_be_sorted.size();    
    uint32_t m = params.num_partitions;
    if(appr_flag){
	m = ceil(params.k/step_size);
	if(m >= params.B - 1){
	    m = params.B - 2;
	}
	num_remaining_matches = params.right_table_size - summed_match;
    }
    
    if(appr_flag || n < step_size){
	if(params.k < step_size){
	    for(const auto & key: keys){
		top_matching_keys.insert(key);
	    }
	    if(!params.debug){ // emulating switching the memory context
	        keys.clear();
	        key_multiplicity.clear();
	    }
	    return 1;
	}
	num_remaining_keys = params.left_table_size - params.k;

    }


    
    std::vector<uint32_t> SumSoFar = std::vector<uint32_t> (key_multiplicity_to_be_sorted.size()+1, 0U);
    for(auto i = 1U; i <= key_multiplicity_to_be_sorted.size(); i++){
	SumSoFar[i] += SumSoFar[i-1] + key_multiplicity_to_be_sorted[i-1].first;
    }
    auto cal_cost = [&](uint32_t start_idx, uint32_t end_idx){
        return (static_cast<uint64_t>(SumSoFar[end_idx+1] - SumSoFar[start_idx]))*(static_cast<uint64_t>(ceil((end_idx - start_idx+1)*1.0/step_size)));
    };

    struct Cut{
        uint64_t cost;
	uint32_t lastPos;
	Cut(){
	    cost = UINT64_MAX;
	    lastPos = 0U;
	}
    };


    
    
   

    double b = static_cast<double>(m+0.5) - static_cast<double>(n*0.5/step_size);
    double b_squared = b*b;

    std::vector<uint32_t> tmp_idxes;
    

    uint32_t tmp_start = static_cast<uint32_t>(floor((n - 1)*1.0/m)) + 1U;
    uint32_t start = n%step_size;
    if(tmp_start/step_size > 0 && !appr_flag){
	start += (tmp_start/step_size - 1)*step_size;
    }
    if(start == 0){
	start = step_size;
    }
    uint32_t tmp_end, tmp;
    uint64_t tmp_cost;

    uint32_t num_steps = (n-start)*1.0/step_size;
    Cut** cut_matrix = new Cut*[num_steps+1];
    for(auto i = 0; i < num_steps+1; i++){
	cut_matrix[i] = new Cut[m + 1];
    }

    for(auto i = 0; i < num_steps; i++){
	cut_matrix[i][1].cost = cal_cost(0,  start+i*step_size);
	cut_matrix[i][1].lastPos = 0;
    }

    uint32_t exact_pos_i, exact_pos_k, tmp_k;  
    uint32_t j_upper_bound = m - 1;
    double tmp_j_upper_bound = m - 1;
    for(auto i = 0; i <= num_steps; i++){
	exact_pos_i = start + i*step_size;
	
	//for(auto j = 2; j <= (exact_pos_i*1.0/n)*m; j++){
        j_upper_bound = m - 1;    
	if(!appr_flag){
	    if(j_upper_bound > i + 1) j_upper_bound = i + 1;
	        if(i != 0){
	            tmp_j_upper_bound = 0.5*b+sqrt(b_squared + 2*m*(exact_pos_i*1.0/step_size-1.0));
	            if(j_upper_bound > tmp_j_upper_bound){
		        j_upper_bound = tmp_j_upper_bound;
	        }
	    }
	}
       
        	
	for(auto j = 2; j <= j_upper_bound; j++){
	    tmp = static_cast<uint32_t>(ceil((n - exact_pos_i - 1)/(m - j))); 
            if(tmp > step_size){
                tmp -= step_size;
		if(tmp >= exact_pos_i) continue;
	        tmp_end = exact_pos_i - tmp;
	    }else{
		tmp_end = exact_pos_i;
	    }

            tmp_start = start;	
	    if(!appr_flag && exact_pos_i > step_size){
	        tmp_start = static_cast<uint32_t> (floor((exact_pos_i-step_size)*(1.0 - 1.0/j)));
	    }
	    
	    if(tmp_start < start){
		tmp_start = start;
	    }else if((tmp_start - start)%step_size != 0){
		tmp_start += step_size - (tmp_start - start)%step_size;
	    }
            
	    if(tmp_end < tmp_start) continue;

	    for(auto k = 0; tmp_start + k*step_size <= tmp_end; k++){
		exact_pos_k = tmp_start + k*step_size;
		tmp_k = (exact_pos_k - start)/step_size;
		if(cut_matrix[tmp_k][j-1].cost == UINT64_MAX || exact_pos_i + cut_matrix[tmp_k][j-1].lastPos > 2*exact_pos_k + 1 + step_size) continue;
		tmp_cost = cal_cost(exact_pos_k+1,exact_pos_i) + cut_matrix[tmp_k][j-1].cost;
		if(tmp_cost < cut_matrix[i][j].cost){
		    cut_matrix[i][j].cost = tmp_cost;
		    cut_matrix[i][j].lastPos = exact_pos_k+1;
		}
	    }
	}
    }
    
    tmp_start = start;
    if(!appr_flag && static_cast<uint32_t>(ceil(n*(1-1.0/m)))> start){
	tmp_start = static_cast<uint32_t>(ceil(n*(1-1.0/m)));
	if((tmp_start - start)%step_size != 0){
	   tmp_start -=  (tmp_start - start)%step_size;
        }
        
    } 
    
    
    uint32_t num_partitions, max_partitions;
    uint64_t min_cost = UINT64_MAX;
    for(exact_pos_k = tmp_start; exact_pos_k < n; exact_pos_k+=step_size){
	tmp_k = (exact_pos_k - start)/step_size;
	for(auto j = 2; j <= m; j++){
	    if(cut_matrix[tmp_k][j-1].cost == UINT64_MAX) break;
	    max_partitions = j-1;
	}
	tmp_cost = cal_cost(exact_pos_k+1,n-1) + cut_matrix[tmp_k][max_partitions].cost;
	if(appr_flag){
	    hash_map_size = get_hash_map_size(params.k, params.K);
	    if(hash_map_size + max_partitions + 1 >= params.B - 1){ // find best mk is implemented together with MatrixDP
		break;
	    }else{
		 m_r = params.B - 2 - hash_map_size - max_partitions;
		 tmp_cost += ceil(num_remaining_keys*1.0/m_r/step_size)*num_remaining_matches;
	    }
	}
	if(tmp_cost < min_cost){
	    num_partitions = max_partitions+1;
	    cut_matrix[num_steps][num_partitions].cost = tmp_cost;
	    cut_matrix[num_steps][num_partitions].lastPos = exact_pos_k+1;
	    min_cost = tmp_cost;
	    //std::cout << "tmp cost : " << tmp_cost << "\t #pre-partitions " << num_partitions << "\t cost:" << cut_matrix[num_steps][max_partitions+1].cost<< std::endl;
	}
    }

    
    //std::cout << "estimated minimum cost (#entries to be scanned in right relation): " << tmp_cost << std::endl;
    

    uint32_t lastPos, tmp_lastPos;
    lastPos = num_steps;
    for(auto j = 0U; j < num_partitions - 1; j++){
        tmp_lastPos = cut_matrix[lastPos][num_partitions-j].lastPos;
	for(auto i = tmp_lastPos - 1; i < lastPos*step_size+start; i++){
	    auto idx = key_multiplicity_to_be_sorted[i].second;
	    partitioned_keys[keys[idx]] = j;
	}
	if(tmp_lastPos == 0) break;

	lastPos = (tmp_lastPos-start-1)/step_size;
    }
    if(lastPos*step_size+start > 0){
	if(num_partitions == 1){
	    for(auto i = 0; i < lastPos*step_size+start; i++){
		top_matching_keys.insert(keys[i]);
	    }
	}else{
	    for(auto i = 0; i < lastPos*step_size+start; i++){
	        auto idx = key_multiplicity_to_be_sorted[i].second;
	        partitioned_keys[keys[idx]] = num_partitions - 1;
            }
	}
        
    }


    for(auto i = 0; i < num_steps+1; i++){
	delete[] cut_matrix[i];
    }
    delete[] cut_matrix;

    if(!params.debug){ // emulating switching the memory context
	   keys.clear();
	   key_multiplicity.clear();
    }
    return num_partitions;
}





void Emulator::get_emulated_cost_MatrixDP(){
    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity; 

    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    load_key_multiplicity(keys, key_multiplicity);
    //std::chrono::time_point<std::chrono::high_resolution_clock>  end2 = std::chrono::high_resolution_clock::now();
    //std::cout << (std::chrono::duration_cast<std::chrono::seconds>(end2 - start)).count() << std::endl;
    std::vector<uint32_t> idxes = std::vector<uint32_t> (params_.left_table_size, 0U);
    for(auto i = 0; i < params_.left_table_size; i++){
	idxes[i] = i;
    }
    get_emulated_cost_MatrixDP(keys, key_multiplicity, idxes, params_.B, params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U);
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}

void Emulator::get_emulated_cost_MatrixDP(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){

    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t num_passes_R = 0;
    num_passes_R = ceil(left_num_entries*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(left_num_entries/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(right_num_entries/right_entries_per_page)){
	if(depth != 0){
               probe_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_BNLJ("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, true);
               probe_end = std::chrono::high_resolution_clock::now();
               probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	}else{
               probe_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
               probe_end = std::chrono::high_resolution_clock::now();
               probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	}
	get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
	return;
    }
    std::unordered_map<std::string, uint16_t> partitioned_keys;
    std::unordered_set<std::string> top_matching_keys; 
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();


    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    uint32_t num_partitions = get_partitioned_keys(keys, key_multiplicity, partitioned_keys, top_matching_keys, params_, false);
    std::chrono::time_point<std::chrono::high_resolution_clock>  algo_end = std::chrono::high_resolution_clock::now();
    algo_duration += (std::chrono::duration_cast<std::chrono::microseconds>(algo_end - partition_start)).count();
    //std::cout << "matrix dp #partitions: " << partitions.size() << std::endl;
 
    std::vector<uint32_t> counter_R = std::vector<uint32_t> (num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (num_partitions, 0U);
    if(depth == 0U){
        system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
        system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
    }
    partition_file(counter_R, partitioned_keys, top_matching_keys, num_partitions, left_file_name, params_.left_E_size, 0, "part_rel_R/", depth); 
    partition_file(counter_S, partitioned_keys, top_matching_keys, num_partitions, right_file_name, params_.right_E_size, 0, "part_rel_S/", depth); 
    
    if(params_.debug){
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
    for(auto i = 0; i < num_partitions; i++){
	if(counter_R[i] == 0 || counter_S[i] == 0){
	    if(counter_R[i] != 0) 
	        remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    if(counter_S[i] != 0) 
	        remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	    continue;
	}
		
	num_passes_R = ceil(counter_R[i]*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
	if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page)){
	//if(num_passes_R <= 1){
	    
	    
            probe_start = std::chrono::high_resolution_clock::now();
	    get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(i), "part_rel_S/" + right_file_name + "-part-" + std::to_string(i), true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	    remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	}else{
	    x+= ceil(counter_R[i]*1.0/left_entries_per_page);
	    get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1); 
	    
	}
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;

}

void Emulator::get_emulated_cost_ApprMatrixDP(){
    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity; 

    std::chrono::time_point<std::chrono::high_resolution_clock>  start = std::chrono::high_resolution_clock::now();
    load_key_multiplicity(keys, key_multiplicity, true);
    //std::chrono::time_point<std::chrono::high_resolution_clock>  end2 = std::chrono::high_resolution_clock::now();
    //std::cout << (std::chrono::duration_cast<std::chrono::seconds>(end2 - start)).count() << std::endl;
    std::vector<uint32_t> idxes = std::vector<uint32_t> (params_.k, 0U);
    for(auto i = 0; i < params_.k; i++){
	idxes[i] = i;
    }
    get_emulated_cost_ApprMatrixDP(keys, key_multiplicity, idxes, params_.B, params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U); // unfinished!!
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}


void Emulator::get_emulated_cost_ApprMatrixDP(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::string left_file_name, std::string right_file_name, uint32_t left_num_entries, uint32_t right_num_entries, uint32_t depth){

    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_start;
    std::chrono::time_point<std::chrono::high_resolution_clock>  probe_end;
    uint32_t num_remaining_entries = left_num_entries - params_.k;
    uint32_t num_passes_R = 0;
    num_passes_R = ceil(left_num_entries*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(left_num_entries/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(right_num_entries/right_entries_per_page)){
	if(depth != 0){
               probe_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_BNLJ("part_rel_R/" + left_file_name, "part_rel_S/" + right_file_name, true);
               probe_end = std::chrono::high_resolution_clock::now();
               probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	}else{
               probe_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
               probe_end = std::chrono::high_resolution_clock::now();
               probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	}
	get_emulated_cost_BNLJ(left_file_name, right_file_name, true);
	return;
    }
    uint32_t num_passes_left_entries = ceil(num_remaining_entries*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
    std::unordered_map<std::string, uint16_t> partitioned_keys;
    std::unordered_set<std::string> top_matching_keys; 
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();


    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    uint32_t num_pre_partitions = get_partitioned_keys(keys, key_multiplicity, partitioned_keys, top_matching_keys, params_, true);
    std::chrono::time_point<std::chrono::high_resolution_clock>  algo_end = std::chrono::high_resolution_clock::now();
    algo_duration += (std::chrono::duration_cast<std::chrono::microseconds>(algo_end - partition_start)).count();
    //std::cout << "matrix dp #partitions: " << num_pre_partitions << std::endl;
 
    if(depth == 0U){
        system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
        system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
    }
    
    if(num_passes_left_entries + num_pre_partitions < params_.num_partitions){
	std::cout << "The number of partitions automatically decreases to " << num_passes_left_entries + num_pre_partitions << " due to the sufficient memory budget."<< std::endl;
	params_.num_partitions = num_passes_left_entries + num_pre_partitions;
    }
    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.num_partitions, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.num_partitions, 0U);
    partition_file(counter_R, partitioned_keys, top_matching_keys, num_pre_partitions, left_file_name, params_.left_E_size, num_passes_left_entries, "part_rel_R/", depth); 
    partition_file(counter_S, partitioned_keys, top_matching_keys, num_pre_partitions, right_file_name, params_.right_E_size, num_passes_left_entries, "part_rel_S/", depth); 
    
    if(params_.debug){
	print_counter_histogram(partitioned_keys, key_multiplicity, keys, num_pre_partitions);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);


    //probing
    uint32_t x = 0;
    for(auto i = 0; i < params_.num_partitions; i++){
	if(counter_R[i] == 0 || counter_S[i] == 0){
	    if(counter_R[i] != 0) 
	        remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    if(counter_S[i] != 0) 
	        remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	    continue;
	}
		
	num_passes_R = ceil(counter_R[i]*1.0/floor(left_entries_per_page*(params_.B - 1 - params_.BNLJ_inner_rel_buffer)/FUDGE_FACTOR));
	if(num_passes_R <= 2 + params_.randwrite_seqread_ratio || 2*ceil(counter_R[i]/left_entries_per_page) >= (num_passes_R - 2 - params_.randwrite_seqread_ratio)*(counter_S[i]/right_entries_per_page)){
	//if(num_passes_R <= 1){
	    
	    
            probe_start = std::chrono::high_resolution_clock::now();
	    get_emulated_cost_BNLJ("part_rel_R/" + left_file_name + "-part-" + std::to_string(i), "part_rel_S/" + right_file_name + "-part-" + std::to_string(i), true);
            probe_end = std::chrono::high_resolution_clock::now();
            probe_duration += (std::chrono::duration_cast<std::chrono::microseconds>(probe_end - probe_start)).count();
	    remove(std::string("part_rel_R/" + left_file_name + "-part-" + std::to_string(i)).c_str());
	    remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	}else{
	    x+= ceil(counter_R[i]*1.0/left_entries_per_page);
	    get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), counter_R[i], counter_S[i], depth + 1); 
	    
	}
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;

}

template uint32_t Emulator::internal_sort<std::greater<std::pair<std::string, std::string>>>(std::string file_name, std::string prefix, uint32_t entry_size);
template uint32_t Emulator::internal_sort<std::less<std::pair<std::string, std::string>>>(std::string file_name, std::string prefix, uint32_t entry_size);
template uint32_t Emulator::merge_sort_for_one_pass<std::greater<std::pair<std::string, uint32_t>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no);
template uint32_t Emulator::merge_sort_for_one_pass<std::less<std::pair<std::string, uint32_t>>>(std::string file_name, std::string prefix, uint32_t entry_size, uint32_t num_runs, uint8_t pass_no);
template void Emulator::merge_join<std::greater<std::pair<std::string, uint32_t>>>(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint32_t left_num_runs, uint32_t right_num_runs, uint8_t left_pass_no, uint8_t right_pass_no);
template void Emulator::merge_join<std::less<std::pair<std::string, uint32_t>>>(std::string left_file_prefix, std::string right_file_prefix, uint32_t left_entry_size, uint32_t right_entry_size, uint32_t left_num_runs, uint32_t right_num_runs, uint8_t left_pass_no, uint8_t right_pass_no);


