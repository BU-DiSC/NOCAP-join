#include "parameters.h"
#include "estimator.h"
#include "emulator.h"

#include <cmath>
#include <iostream>
#include <vector>
#include <deque>
#include <fstream>
#include <string>
#include <unordered_map>
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

void Emulator::print_counter_histogram(std::vector< std::vector<uint32_t> > & partitions, std::vector<uint32_t> & key_multiplicity){
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, uint32_t> > histogram;
    std::unordered_map<uint32_t, uint32_t> counter;
    uint32_t count = 0;
    for(uint32_t i = 0; i < partitions.size(); i++){
	count = partitions[i].size();
        if(histogram.find(count) == histogram.end()){
	    histogram[count] = std::unordered_map<uint32_t, uint32_t> ();
	}
	if(counter.find(count) == counter.end()){
	    counter[count] = 0;
	}
	counter[count]++;
	for(auto & id: partitions[i]){
	    if(histogram[count].find(key_multiplicity[id]) == histogram[count].end()){
		histogram[count][key_multiplicity[id]] = 0;
	    }
	    histogram[count][key_multiplicity[id]]++;
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
	//memset(join_output_buffer, 0, DB_PAGE_SIZE);
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
	random_io_cnt++;
	random_load_in_probe_cnt++;
	
        for(auto i = 0; i < params_.B - 1 - params_.BNLJ_inner_rel_buffer; i++){
	        read_bytes_R = read_one_page(fd_R, R_rel_buffer + i*DB_PAGE_SIZE);
	        if(read_bytes_R <= 0){
	    	    end_flag_R = true;
		    break;
	        }
                tmp_cnt++;
	        R_num_pages_in_buff++;
	        src2_end_addr += DB_PAGE_SIZE;
        }

	/*
	if(hash){
	    key2Rvalue.clear(); 
            for(auto i = 0; i < params_.B - 1 - params_.BNLJ_inner_rel_buffer; i++){
	        read_bytes_R = read_one_page(fd_R, R_rel_buffer + i*DB_PAGE_SIZE);
                if(read_bytes_R <= 0){
	    	    end_flag_R = true;
		    break;
	        }
		tmp_buffer = R_rel_buffer + i*DB_PAGE_SIZE;
                for(auto j = 0; j < left_entries_per_page; j++){
		    if(*(tmp_buffer+j*params_.left_E_size) == '\0') break;
		    tmp_key = std::string(tmp_buffer + j*params_.left_E_size, params_.K); 
		    tmp_value = std::string(tmp_buffer + j*params_.left_E_size + params_.K, left_value_size);
		    key2Rvalue[tmp_key] = tmp_value;
		}
	        
                tmp_cnt++;
	        R_num_pages_in_buff++;
	        src2_end_addr += DB_PAGE_SIZE;
            }
	}else{
            for(auto i = 0; i < params_.B - 1 - params_.BNLJ_inner_rel_buffer; i++){
	        read_bytes_R = read_one_page(fd_R, R_rel_buffer + i*DB_PAGE_SIZE);
	        if(read_bytes_R <= 0){
	    	    end_flag_R = true;
		    break;
	        }
                tmp_cnt++;
	        R_num_pages_in_buff++;
	        src2_end_addr += DB_PAGE_SIZE;
            }
	}*/
        
    //auto tmp_end = std::chrono::high_resolution_clock::now();
    //tmp_duration += (std::chrono::duration_cast<std::chrono::microseconds>(tmp_end - tmp_start)).count();
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
	    random_io_cnt++;
	    random_load_in_probe_cnt++;
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

void Emulator::partition_file(std::vector<uint32_t> & counter, const std::unordered_map<std::string, uint32_t> & partitioned_keys, uint32_t num_pre_partitions, std::string file_name, uint32_t entry_size, uint32_t divider, std::string prefix, uint32_t depth){
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
	random_io_cnt++;
	read_bytes = read_one_page(fd, buffer);
	if(read_bytes <= 0) break;

	for(auto i = 0; i < entries_per_page; i++){
	    tmp_str = std::string(buffer + i*entry_size, params_.K);
	    if(!prepartitioned){
                hash_value = Estimator::get_hash_value(tmp_str, tmp_ht, Estimator::s_seed);
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
		if(partitioned_keys.find(tmp_str) == partitioned_keys.end()){
		    if(divider != 0){
                        hash_value = Estimator::get_hash_value(tmp_str, tmp_ht, Estimator::s_seed);
                        subpartition_idx = (hash_value%(divider))%(params_.num_partitions - num_pre_partitions);
			/*
		        if(subpartition_idx >= lb_partition_id_th){
			    subpartition_idx = (subpartition_idx - lb_partition_id_th)%(params_.num_partitions - lb_partition_id_th - num_pre_partitions) + lb_partition_id_th;
		        }*/
			subpartition_idx += num_pre_partitions;
		    }
	            continue; // ignore keys if they are not in the given partition map
		}else{
		    subpartition_idx = partitioned_keys.at(tmp_str);
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
        partition_file(counter_R, {}, 0, left_file_name, params_.left_E_size, num_passes_R, "part_rel_R/", depth); 
        partition_file(counter_S, {}, 0, right_file_name, params_.right_E_size, num_passes_R, "part_rel_S/", depth); 
    }else{
        partition_file(counter_R, {}, 0, left_file_name, params_.left_E_size, 0, "part_rel_R/", depth); 
        partition_file(counter_S, {}, 0, right_file_name, params_.right_E_size,0, "part_rel_S/", depth); 
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
	random_io_cnt++;
	read_bytes = read_one_page(fd_R, input_buffer);
	if(read_bytes <= 0) break;

	for(auto i = 0; i < left_entries_per_page; i++){
	    tmp_str = std::string(input_buffer + i*params_.left_E_size, params_.K);
            hash_value = Estimator::get_hash_value(tmp_str, tmp_ht, Estimator::s_seed);
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
	random_io_cnt++;
	read_bytes = read_one_page(fd_S, input_buffer);
	if(read_bytes <= 0) break;

	for(auto j = 0; j < right_entries_per_page; j++){
	    tmp_str = std::string(input_buffer + j*params_.right_E_size, params_.K);
            hash_value = Estimator::get_hash_value(tmp_str, tmp_ht, Estimator::s_seed);
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
	    /*
            if(depth == 0){
               auto tmp_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(subpartition_idx), right_file_name + "-part-" + std::to_string(subpartition_idx), depth + 1);
               auto tmp_end = std::chrono::high_resolution_clock::now();
               tmp_duration += (std::chrono::duration_cast<std::chrono::microseconds>(tmp_end - tmp_start)).count();
	   }else{
	       get_emulated_cost_HP(left_file_name + "-part-" + std::to_string(subpartition_idx), right_file_name + "-part-" + std::to_string(subpartition_idx), depth + 1);
	   }*/
	}
    }
    if(x > 0) std::cout << "repartitioned cost:" << x << std::endl;
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
    std::unordered_map<std::string, uint32_t> partitioned_keys;
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();

    std::vector<uint32_t> counter_R = std::vector<uint32_t> (params_.B - 1, 0U);
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (params_.B - 1, 0U);

    double pre_io_duration = io_duration;
    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_start = std::chrono::high_resolution_clock::now();
    Estimator::get_partitioned_keys(keys, key_multiplicity, idxes, buffer_in_pages, partitioned_keys, partitions, params_, depth);
    std::chrono::time_point<std::chrono::high_resolution_clock>  algo_end = std::chrono::high_resolution_clock::now();
    algo_duration += (std::chrono::duration_cast<std::chrono::microseconds>(algo_end - partition_start)).count();
    //std::cout << "matrix dp #partitions: " << partitions.size() << std::endl;
    //std::cout << partitioned_keys.size() << std::endl;
 
    if(depth == 0U){
        system("mkdir -p part_rel_R/; rm -rf part_rel_R/;mkdir -p part_rel_R/");
        system("mkdir -p part_rel_S/; rm -rf part_rel_S/;mkdir -p part_rel_S/");
    }
    partition_file(counter_R, partitioned_keys, 0, left_file_name, params_.left_E_size, 0, "part_rel_R/", depth); 
    partition_file(counter_S, partitioned_keys, 0, right_file_name, params_.right_E_size, 0, "part_rel_S/", depth); 
    /* 
    if(depth == 0U){
	print_counter_histogram(partitions, key_multiplicity);
    }*/

    std::chrono::time_point<std::chrono::high_resolution_clock>  partition_end = std::chrono::high_resolution_clock::now();
    double tmp_partition_duration = (std::chrono::duration_cast<std::chrono::microseconds>(partition_end - partition_start)).count();
    partition_duration += tmp_partition_duration; 
    double curr_io_duration = io_duration;
    partition_io_duration += curr_io_duration - pre_io_duration;
    partition_cpu_duration += tmp_partition_duration - (curr_io_duration - pre_io_duration);


    //probing
    uint32_t x = 0;
    for(auto i = 0; i < params_.B - 1; i++){
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
	remove(std::string("part_rel_S/" + right_file_name + "-part-" + std::to_string(i)).c_str());
	*/

	
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
	    /*
            if(depth == 0){
               auto tmp_start = std::chrono::high_resolution_clock::now();
	       get_emulated_cost_MatrixDP(keys, key_multiplicity, partitions[i], params_.B, left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), depth + 1); 
               auto tmp_end = std::chrono::high_resolution_clock::now();
               tmp_duration += (std::chrono::duration_cast<std::chrono::microseconds>(tmp_end - tmp_start)).count();
	   }else{
	       get_emulated_cost_MatrixDP(keys, key_multiplicity, partitions[i], params_.B, left_file_name + "-part-" + std::to_string(i), right_file_name + "-part-" + std::to_string(i), depth + 1); 
	   }*/
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
    get_emulated_cost_MatrixDP(keys, key_multiplicity, idxes, params_.B, params_.workload_rel_R_path, params_.workload_rel_S_path, params_.left_table_size, params_.right_table_size, 0U); // unfinished!!
    finish();
    std::chrono::time_point<std::chrono::high_resolution_clock>  end = std::chrono::high_resolution_clock::now();
    join_duration = (std::chrono::duration_cast<std::chrono::microseconds>(end - start)).count();
}
