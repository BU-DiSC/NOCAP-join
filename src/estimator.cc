#include <set>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <math.h>
#include <utility>
#include <algorithm>
#include <cstdint>

#include "estimator.h"
#include "hash/md5.h"
#include "hash/murmurhash.h"
#include "hash/Crc32.h"
#include "hash/sha-256.h"
#include "hash/xxhash.h"
#include "hash/citycrc.h"
#include "hash/robin_hood.h"
#include <functional>

#include <iostream>

uint32_t Estimator::s_seed = 0xbc9f1d34;

Estimator::Estimator(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params){
    keys_ = keys;
    key_multiplicity_ = key_multiplicity;
    params_ = params;
    repartitioned_keys = vector<uint32_t>(7, 0U);
    left_entries_per_page = params.page_size/params.left_E_size;
    right_entries_per_page = params.page_size/params.right_E_size;
}

uint64_t Estimator::get_hash_value(std::string & key, HashType & ht, uint32_t seed){
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

uint64_t Estimator::get_estimated_io(){
    std::vector<uint32_t> idxes = std::vector<uint32_t> (params_.left_table_size, 0U);
    for(auto i = 0; i < params_.left_table_size; i++){
	idxes[i] = i;
    }
    uint64_t cost = 0U;
    switch(params_.pjm){
        case BNLJ:
	    cost = get_estimated_io_BNLJ(idxes);
	    break;
	case Hash:
	    cost = get_estimated_io_HP(idxes);
	    break;
	case MatrixDP:
	    cost = get_estimated_io_MatrixDP(idxes);
	    break;
	case Hybrid:
	    cost = get_estimated_io_Hybrid(idxes);
	    break;
	default:
	    cost = 1U;
	    break;
    }
    io_duration = cost*params_.io_latency;
    return cost;
}

uint64_t Estimator::get_estimated_io_BNLJ(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth){
    uint64_t cost = 0;
    uint32_t right_table_scanned_entires = 0;
    for(auto idx: idxes){
	right_table_scanned_entires += key_multiplicity_[idx];
    }    
    uint64_t right_table_scanned_cost = static_cast<uint64_t>(ceil(right_table_scanned_entires*1.0/right_entries_per_page));
    uint32_t blk_start_idx = 0;
    uint32_t blk_end_idx = 0;
    uint32_t accumulated_num_right_entries = 0;
    while(blk_start_idx < idxes.size()){
	accumulated_num_right_entries = 0;
	blk_end_idx = blk_start_idx + left_entries_per_page*(buffer_in_pages-2); 
	if(blk_end_idx >= params_.left_table_size){
	    blk_end_idx = params_.left_table_size - 1;
	}
	cost += right_table_scanned_cost;
	blk_start_idx = blk_end_idx + 1;
    }
    cost += static_cast<uint64_t>(ceil(idxes.size()*1.0/left_entries_per_page));
    return cost;
}

uint64_t Estimator::get_estimated_io_HP(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth){
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > (buffer_in_pages - 1U, std::vector<uint32_t>());
    std::vector<uint32_t> counter_S = std::vector<uint32_t>(buffer_in_pages - 1U, 0);
    uint64_t hash_value; 
    HashType tmp_ht = params_.ht;
    tmp_ht = static_cast<HashType> ((tmp_ht + depth)%6U);
    std::chrono::time_point<std::chrono::high_resolution_clock>  cpu_start = std::chrono::high_resolution_clock::now();
    uint16_t partition_id = 0;
    for(auto i = 0U; i < idxes.size(); i++){
        hash_value = get_hash_value(keys_[idxes[i]], tmp_ht, Estimator::s_seed);
	partition_id = hash_value%(buffer_in_pages - 1U);
	partitions[partition_id].push_back(idxes[i]);

	counter_S[partition_id] += key_multiplicity_[idxes[i]];
    }

    uint64_t cost = 0;
    uint32_t accumulated_num_right_entries = 0;

    // cost in partitioning phase
    cost += 2*(static_cast<uint64_t>(idxes.size()*1.0/left_entries_per_page));
    for(auto idx: idxes){
        accumulated_num_right_entries += key_multiplicity_[idx];
    }
    cost += 2*(static_cast<uint64_t>(accumulated_num_right_entries*1.0/right_entries_per_page));
    //std::cout << "scanning cost : " << cost << std::endl;
    //std::cout << "Partitioning phase in depth " << (static_cast<int>(depth)) << " takes " << cost << " I/Os" << std::endl;
    std::chrono::time_point<std::chrono::high_resolution_clock>  cpu_end = std::chrono::high_resolution_clock::now();
    //cpu_duration += (1.0 + accumulated_num_right_entries*1.0/idxes.size())* (std::chrono::duration_cast<std::chrono::microseconds>(cpu_end - cpu_start)).count();
    cpu_duration +=  (std::chrono::duration_cast<std::chrono::microseconds>(cpu_end - cpu_start)).count();

    // cost in probing phase
    uint64_t delta_cost;
    uint64_t n = idxes.size();
    uint64_t m = buffer_in_pages - 1U;
    double threshold = 0;
    if(params_.hybrid && depth == 0U){
	threshold = (1.0 + n*1.0/(params_.c*m))*log(params_.c + n*1.0/m);
	//std::cout << "threshold : " << threshold << "\t" << " average : " << n*1.0/m << std::endl;
    }
    uint32_t repartition_counts = 0;
    uint32_t fudged_entries_per_buffer = floor(left_entries_per_page*(params_.B - 2)/FUDGE_FACTOR);
    for(auto j = 0U; j < buffer_in_pages - 1U; j++){
	delta_cost = 0U;
        if(2.0*partitions[j].size() >= counter_S[j]*(ceil(partitions[j].size()/fudged_entries_per_buffer) - 3)){
        //if(partitions[j].size() <= fudged_entries_per_buffer){
	    delta_cost += static_cast<uint64_t>(ceil(partitions[j].size()*1.0/left_entries_per_page));
            accumulated_num_right_entries = 0;
	    for(auto idx:partitions[j]){
                accumulated_num_right_entries += key_multiplicity_[idx];
	    }
	    delta_cost += static_cast<uint64_t>(ceil(accumulated_num_right_entries*1.0/right_entries_per_page));
	}else{
	    
	    if(params_.hybrid && partitions[j].size() >= threshold){
                delta_cost = get_estimated_io_MatrixDP(partitions[j], params_.B, depth+1);
	    }else{
	        
		//delta_cost = get_estimated_io_BNLJ(partitions[j], depth+1);
		
		
	        if(depth > 6U){
		    delta_cost = get_estimated_io_BNLJ(partitions[j], params_.B, depth+1);
	        }else{
	    	    delta_cost= get_estimated_io_HP(partitions[j], depth+1);
	        }
	    }
	    repartitioned_keys[depth]+= delta_cost;
	    repartition_counts++;

	}
	
	cost += delta_cost;
    }
    if(depth == 0U) std::cout << "Repartition count: " << repartition_counts << std::endl;
    return cost;
}



uint64_t Estimator::get_estimated_io_Hybrid(std::vector<uint32_t> & idxes, uint32_t buffer_in_pages,uint8_t depth){
    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;
    std::map<std::string, uint32_t> partitioned_keys;
    std::chrono::time_point<std::chrono::high_resolution_clock>  cpu_start = std::chrono::high_resolution_clock::now();
    for(auto idx:idxes){
	if(key_multiplicity_[idx] == 0) continue;
	key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity_[idx], idx));
    }
    uint32_t n = key_multiplicity_to_be_sorted.size();
    std::sort(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    uint32_t num_left_entries_per_pass = left_entries_per_page*(buffer_in_pages-2);
    if(params_.th >= 1) return get_estimated_io_MatrixDP(idxes, buffer_in_pages, depth);
    uint32_t n_for_matrixDP = n - ceil((1.0-params_.th)*n/num_left_entries_per_pass)*num_left_entries_per_pass;
    uint32_t buffer_for_matrixDP = floor(n_for_matrixDP*(buffer_in_pages-2)*1.0/n);
    //uint32_t buffer_for_hash = ceil((n - n_for_matrixDP)*1.0/num_left_entries_per_pass);
    if(buffer_for_matrixDP < 2) return get_estimated_io_HP(idxes, buffer_in_pages, depth);
    if(buffer_for_matrixDP > buffer_in_pages - 2) return get_estimated_io_MatrixDP(idxes, buffer_in_pages, depth);
    //uint32_t buffer_for_hash = ceil((n - n_for_matrixDP)*1.0/num_left_entries_per_pass);
    uint32_t buffer_for_hash = buffer_in_pages - buffer_for_matrixDP - 2; 
    //uint32_t buffer_for_matrixDP = buffer_in_pages - buffer_for_hash; 
    
    std::vector<uint32_t> idxes_for_matrixDP = vector<uint32_t> (n_for_matrixDP, 0U);
    std::vector<uint32_t> idxes_for_hash = vector<uint32_t> (n - n_for_matrixDP, 0U);
    for(int i = 0; i < n - n_for_matrixDP; i++){ 
	idxes_for_hash[i] = key_multiplicity_to_be_sorted[i + n_for_matrixDP].second;
    }
    for(int i = 0; i < n_for_matrixDP; i++){
	idxes_for_matrixDP[i] = key_multiplicity_to_be_sorted[i].second;
    }
    cout << "splitting pages in buffer into : " << buffer_for_matrixDP  << " " << buffer_in_pages - buffer_for_matrixDP << std::endl;
    return get_estimated_io_MatrixDP(idxes_for_matrixDP,  buffer_for_matrixDP + 2U, depth) + get_estimated_io_HP(idxes_for_hash, buffer_in_pages - buffer_for_matrixDP + 2U, depth);


}


void Estimator::get_partitioned_keys(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, std::vector<uint32_t> & idxes, uint32_t buffer_in_pages, std::unordered_map<std::string, uint32_t> & partitioned_keys, std::vector<std::vector<uint32_t> > & partitions, Params & params, uint8_t depth){
    partitioned_keys.clear();
    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;

    for(auto idx:idxes){
	if(key_multiplicity[idx] == 0) continue;
	key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity[idx], idx));
    }
    std::reverse(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    
    std::vector<uint32_t> SumSoFar = std::vector<uint32_t> (idxes.size()+1, 0U);
    for(auto i = 1U; i <= key_multiplicity_to_be_sorted.size(); i++){
	SumSoFar[i] += SumSoFar[i-1] + key_multiplicity_to_be_sorted[i-1].first;
    }
    uint32_t left_entries_per_page = params.page_size/params.left_E_size;
    uint32_t num_left_entries_per_pass = floor(left_entries_per_page*(buffer_in_pages-2)/FUDGE_FACTOR);
    auto cal_cost = [&](uint32_t start_idx, uint32_t end_idx){
        return (static_cast<uint64_t>(SumSoFar[end_idx+1] - SumSoFar[start_idx]))*(static_cast<uint64_t>(ceil((end_idx - start_idx+1)*1.0/num_left_entries_per_pass)));
    };

    struct Cut{
        uint64_t cost;
	uint32_t lastPos;
	Cut(){
	    cost = UINT64_MAX;
	    lastPos = 0U;
	}
    };
    uint32_t n = key_multiplicity_to_be_sorted.size();    
    uint32_t m = buffer_in_pages - 1;
   
    uint32_t step_size = (uint32_t) floor(left_entries_per_page*(params.B - 1 - params.BNLJ_inner_rel_buffer)/FUDGE_FACTOR);
    if(depth == 0U){
	std::cout << "step size: " << step_size << std::endl;
    }

    double b = static_cast<double>(m+0.5) - static_cast<double>(n*0.5/step_size);
    double b_squared = b*b;

    std::vector<uint32_t> tmp_idxes;
    if(step_size >= n){ // return an empty partition map, indicating BLNJ
        return;
    }

    uint32_t tmp_start = static_cast<uint32_t>(floor((n - 1)*1.0/m)) + 1U;
    uint32_t start = n%step_size;
    if(tmp_start/step_size > 0){
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
	if(j_upper_bound > i + 1) j_upper_bound = i + 1;
	if(i != 0){
	    tmp_j_upper_bound = 0.5*b+sqrt(b_squared + 2*m*(exact_pos_i*1.0/step_size-1.0));
	    if(j_upper_bound > tmp_j_upper_bound){
		j_upper_bound = tmp_j_upper_bound;
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
	    if(exact_pos_i > step_size){
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
     
    tmp_start = static_cast<uint32_t>(ceil(n*(1-1.0/m)));
    if(tmp_start > start){
	if((tmp_start - start)%step_size != 0){
	    tmp_start -=  (tmp_start - start)%step_size;
        }
    }else{
	tmp_start = start;
    }
    
    uint32_t num_partitions, max_partitions;
    for(exact_pos_k = tmp_start; exact_pos_k < n; exact_pos_k+=step_size){
	tmp_k = (exact_pos_k - start)/step_size;
	for(auto j = 2; j <= m; j++){
	    if(cut_matrix[tmp_k][j-1].cost == UINT64_MAX) break;
	    max_partitions = j-1;
	    
	}
	tmp_cost = cal_cost(exact_pos_k+1,n-1) + cut_matrix[tmp_k][max_partitions].cost;
	if(tmp_cost < cut_matrix[num_steps][max_partitions+1].cost){
	        cut_matrix[num_steps][max_partitions+1].cost = tmp_cost;
	        cut_matrix[num_steps][max_partitions+1].lastPos = exact_pos_k+1;
		num_partitions = max_partitions+1;
	}
    }

    if(depth == 0)
        std::cout << "estimated minimum cost (#entries to be scanned in right relation): " << tmp_cost << std::endl;

    uint32_t lastPos, tmp_lastPos;
    lastPos = num_steps;
    partitions = std::vector<std::vector<uint32_t> > (num_partitions, std::vector<uint32_t> ());
    for(auto j = 0U; j < num_partitions - 1; j++){
        tmp_lastPos = cut_matrix[lastPos][num_partitions-j].lastPos;
	
	for(auto i = tmp_lastPos - 1; i < lastPos*step_size+start; i++){
	    auto idx = key_multiplicity_to_be_sorted[i].second;
	    partitioned_keys[keys[idx]] = j;
	    partitions[j].push_back(idx);
	}

	if(tmp_lastPos == 0) break;
	lastPos = (tmp_lastPos-start-1)/step_size;
    }
    if(start > 0){
        for(auto i = 0; i < start; i++){
	    auto idx = key_multiplicity_to_be_sorted[i].second;
	    partitioned_keys[keys[idx]] = num_partitions - 1;
	    partitions[num_partitions-1].push_back(idx);
        }
    }


    for(auto i = 0; i < num_steps+1; i++){
	delete[] cut_matrix[i];
    }
    delete[] cut_matrix;
}


uint64_t Estimator::get_estimated_io_MatrixDP(std::vector<uint32_t> & idxes,uint32_t buffer_in_pages, uint8_t depth){
    std::unordered_map<std::string, uint32_t> partitioned_keys;
    std::vector<std::vector<uint32_t> > partitions = std::vector<std::vector<uint32_t> > ();
    std::chrono::time_point<std::chrono::high_resolution_clock>  cpu_start = std::chrono::high_resolution_clock::now();

    get_partitioned_keys(keys_, key_multiplicity_, idxes, buffer_in_pages, partitioned_keys, partitions, params_, depth);

    if(partitioned_keys.size() == 0){
	std::vector<uint32_t> tmp_idxes;
	for(auto idx:idxes){
	    if(key_multiplicity_[idx] != 0){
		tmp_idxes.push_back(idx);
	    }
	}
	return get_estimated_io_BNLJ(tmp_idxes, buffer_in_pages, depth);
    }

    uint64_t real_cost = 0U;
    uint32_t accumulated_num_right_entries = 0U;;
    // cost in partitioning phase
    real_cost += 2*(static_cast<uint64_t>(idxes.size()*1.0/left_entries_per_page));
    for(auto idx: idxes){
        accumulated_num_right_entries += key_multiplicity_[idx];
    }
    real_cost += 2*(static_cast<uint64_t>(accumulated_num_right_entries*1.0/right_entries_per_page));
    uint32_t step_size = floor(left_entries_per_page*(params_.B - 2)/FUDGE_FACTOR);
    uint32_t num_partitions = partitions.size();
    uint64_t tmp_cost = 0U;
    std::vector<uint32_t> counter_S = std::vector<uint32_t> (partitions.size(), 0);
    uint32_t partitioned_counter_S = 0;
    for(auto j = 0U; j < num_partitions; j++){
	partitioned_counter_S = 0;
	for(auto idx:partitions[j]){
	    partitioned_counter_S += key_multiplicity_[idx];
	}	
	if(2.0*partitions[j].size() < partitioned_counter_S*(ceil(partitions[j].size()/step_size) - 3)){
	//if(partitions[j].size() > step_size){
	    if(repartitioned_keys.size() < depth+1){
	        repartitioned_keys.push_back(0);
	    }
	    repartitioned_keys[depth]+= tmp_cost;
	    tmp_cost = get_estimated_io_MatrixDP(partitions[j], params_.B, depth+1);
	}else{
	    tmp_cost = get_estimated_io_BNLJ(partitions[j], params_.B, depth+1);
	}
	real_cost += tmp_cost ;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock>  cpu_end = std::chrono::high_resolution_clock::now();
    cpu_duration +=  (std::chrono::duration_cast<std::chrono::microseconds>(cpu_end - cpu_start)).count();

    



    
    return real_cost;
}
