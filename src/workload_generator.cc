#include <vector>
#include <set>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <cstring>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "args.hxx"
#include "parameters.h"
#include "dist_generator.h"


const char value_alphanum[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; 

std::string get_random_string(uint32_t size);
void generate_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params);
void dump_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params params);
int parse_arguments(int argc, char *argv[], Params & params);

int main(int argc, char *argv[]){
    Params params;
    if(parse_arguments(argc, argv, params)){
        exit(1);
    }
    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity;
    generate_workload(keys, key_multiplicity, params);
    dump_workload(keys, key_multiplicity, params);
    return 0;
}


std::string get_random_string(uint32_t size){
    char* s = new char[size+1];
    for (int i = 0; i < size; ++i) {
        s[i] = value_alphanum[rand() % (sizeof(value_alphanum) - 1)];
    }
    s[size] = '\0';
    return s;
}



int parse_arguments(int argc, char *argv[], Params & params){
    args::ArgumentParser parser("workload_gen_parser", "");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
    args::ValueFlag<long> left_table_size_cmd(group1, "lTS", "the size of the left table to be joined [def: 1000000]", {"lTS"});
    args::ValueFlag<long> right_table_size_cmd(group1, "rTS", "the size of the right table to be joined [def: 8000000]", {"rTS"});
    args::ValueFlag<uint32_t> key_size_cmd(group1, "Join key size", "[def: 8]", {"join-key-size"});
    args::ValueFlag<uint32_t> left_E_cmd(group1, "lE", "the entry size (in bytes) in the left table [def: 1024]", {"lE"});
    args::ValueFlag<uint32_t> right_E_cmd(group1, "rE", "the entry size (in bytes) in the left table [def: 1024]", {"rE"});

    args::ValueFlag<std::string> workload_path_dis_cmd(group1, "path", "the workload distribution path [def: ./workload-dis.txt]", {"path-dis"});
    args::ValueFlag<std::string> workload_path_rel_R_cmd(group1, "path", "the path for relation R [def: ./workload-rel-R.dat]", {"path-rel-R"});
    args::ValueFlag<std::string> workload_path_rel_S_cmd(group1, "path", "the path for relation S [def: ./workload-rel-S.dat]", {"path-rel-S"});
    //distribution params
    args::ValueFlag<uint32_t> join_dist_cmd(group1, "JD", "Join Distribution [0: uniform, 1:normal, 2:beta, 3:zipf, def: 0]", {"JD", "join_distribution"});
    args::ValueFlag<float> join_dist_norm_stddev_cmd(group1, "JD_Norm_Stddev", ", def: 1.0]", {"JD_NDEV", "join_distribution_norm_standard_deviation"});
    args::ValueFlag<float> join_dist_beta_alpha_cmd(group1, "JD_Beta_Alpha", ", def: 1.0]", {"JD_BALPHA", "join_distribution_beta_alpha"});
    args::ValueFlag<float> join_dist_beta_beta_cmd(group1, "JD_Beta_Beta", ", def: 1.0]", {"JD_BBETA", "join_distribution_beta_beta"});
    args::ValueFlag<float> join_dist_zipf_alpha_cmd(group1, "JD_Zipf_Alpha", ", def: 1.0]", {"JD_ZALPHA", "join_distribution_zipf_alpha"});


 
    try {
        parser.ParseCLI(argc, argv);
    }
    catch (args::Help&) {
        std::cout << parser;
        exit(0);
        // return 0;
     }
     catch (args::ParseError& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
	return 1;
     }
     catch (args::ValidationError& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
	return 1;
     }
     params.left_table_size = left_table_size_cmd ? args::get(left_table_size_cmd) : 1000000;
     params.right_table_size = right_table_size_cmd ? args::get(right_table_size_cmd) : 8000000;
     params.join_key_size = key_size_cmd ? args::get(key_size_cmd) : 8; 
     params.left_E_size = left_E_cmd ? args::get(left_E_cmd) : 1024;
     params.right_E_size = right_E_cmd ? args::get(right_E_cmd) : 1024;
     std::cout << params.join_key_size << " " << params.left_E_size << " " << params.right_E_size << std::endl;

     params.workload_dis_path = workload_path_dis_cmd ? args::get(workload_path_dis_cmd) : "./workload-dis.txt";
     params.workload_rel_R_path = workload_path_rel_R_cmd ? args::get(workload_path_rel_R_cmd) : "./workload-rel-R.dat";
     params.workload_rel_S_path = workload_path_rel_S_cmd ? args::get(workload_path_rel_S_cmd) : "./workload-rel-S.dat";

     //distribution params
     uint32_t dist_number = join_dist_cmd ? args::get(join_dist_cmd) : 0;
     switch(dist_number){
	     case 0U:
		     params.join_dist = UNIFORM;
		     break;
             case 1U:
		     params.join_dist = NORMAL;
		     break;
             case 2U:
		     params.join_dist = ZIPFIAN;
		     break;
	     case 3U:
		     params.join_dist = BETA;
		     break;
             default:
                     std::cerr << "\033[0;31m Error: \033[0m The dist parameter should be an integer between [0,3]" << std::endl;
		     return 1;
     }

     params.join_dist_norm_stddev = join_dist_norm_stddev_cmd ? args::get(join_dist_norm_stddev_cmd) : 1;
     params.join_dist_beta_alpha = join_dist_beta_alpha_cmd ? args::get(join_dist_beta_alpha_cmd) : 1;
     params.join_dist_beta_beta = join_dist_beta_beta_cmd ? args::get(join_dist_beta_beta_cmd) : 1;
     params.join_dist_zipf_alpha = join_dist_zipf_alpha_cmd ? args::get(join_dist_zipf_alpha_cmd) : 1;
		     


     return 0;
	
}



void generate_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params){
    std::srand((unsigned int)std::time(NULL));
    std::set<std::string> keys_set;
    keys = std::vector<std::string> (params.left_table_size, "");
    key_multiplicity = std::vector<uint32_t> (params.left_table_size, 0);
    for(auto i = 0; i < keys.size(); i++){
	std::string tmp_key = get_random_string(params.join_key_size);
	while(keys_set.find(tmp_key) != keys_set.end()){
            tmp_key = get_random_string(params.join_key_size); 
	}
	keys[i] = tmp_key;
    }

    DistGenerator dist_generator = DistGenerator(params.join_dist, params.left_table_size, params.join_dist_norm_stddev, params.join_dist_beta_alpha, params.join_dist_beta_beta, params.join_dist_zipf_alpha);
    for(auto i = 0; i < params.right_table_size; i++){
	key_multiplicity[dist_generator.getNext()]++;
    }

}


void dump_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params params){
    std::ofstream fp;
    fp.open(params.workload_dis_path.c_str());
    // 4 => STRING as join key type
    fp << params.left_table_size << " " << params.right_table_size << " 4 " << params.join_key_size << " " << params.left_E_size << " " << params.right_E_size << std::endl;

    std::vector<std::pair<uint32_t, uint32_t> > key_multiplicity_to_be_sorted;

    for(int idx = 0; idx < keys.size(); idx++){
	//if(key_multiplicity[idx] == 0) continue;
	key_multiplicity_to_be_sorted.push_back(std::make_pair(key_multiplicity[idx], idx));
    }
    std::sort(key_multiplicity_to_be_sorted.begin(), key_multiplicity_to_be_sorted.end());
    

    std::set<std::string> keys_set;
    for(auto i = key_multiplicity_to_be_sorted.size(); i > 0 ; i--){
	fp << keys[key_multiplicity_to_be_sorted[i-1].second] << " " << key_multiplicity_to_be_sorted[i-1].first  << std::endl;
    }
    fp.close();
    
    int tuples_per_page = 0; 
    int start = 0;
    int end = 0;
    char buffer[DB_PAGE_SIZE];
    buffer[DB_PAGE_SIZE] = '\0';
    std::string tmp_entry;
    // flush relation S
    
    std::ofstream fp_S (params.workload_rel_S_path.c_str(), std::ofstream::binary);
    //fp_S.open(params.workload_rel_S_path.c_str(), std::ios::out | std::ios::binary);
    tuples_per_page = DB_PAGE_SIZE/(params.right_E_size);
    int domain_size = keys.size();
    int right_value_size = params.left_E_size - params.join_key_size;
    int idx = 0;
    while(idx < domain_size){
	if(key_multiplicity[idx] == 0){
	    domain_size--;
	    key_multiplicity[idx] = key_multiplicity[domain_size];
	    //keys[idx] = keys[domain_size];
            std::swap(keys[idx], keys[domain_size]);
	}else{
	    idx++;
	}
    }
    
    start = 0;
    while(start < params.right_table_size){
	memset(buffer, 0, DB_PAGE_SIZE);
	if(start + tuples_per_page >= params.right_table_size) tuples_per_page = params.right_table_size - start;
	for(auto i = 0; i < tuples_per_page; i++){
	    idx = rand()%domain_size;
	    if(keys_set.find(keys[idx]) != keys_set.end()){
		continue;
	    }
	    tmp_entry = get_random_string(right_value_size);
            strcpy(buffer+i*(params.right_E_size), keys[idx].c_str());
            strcpy(buffer+i*(params.right_E_size)+params.join_key_size, tmp_entry.c_str());
	    tmp_entry.clear();
	    key_multiplicity[idx]--;
	    if(key_multiplicity[idx] == 0){
		domain_size--;
		key_multiplicity[idx] = key_multiplicity[domain_size];
		//std::swap(key_multiplicity[idx], key_multiplicity[domain_size]);
		std::swap(keys[idx], keys[domain_size]);
	    }
	    start++;
	}
	
	fp_S.write((char*)&buffer[0], DB_PAGE_SIZE);
	    
    }
    fp_S.flush();
    fp_S.close();


    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    // flush relation R
    shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));
    tuples_per_page = DB_PAGE_SIZE/(params.left_E_size);

    start = 0;
    std::ofstream fp_R (params.workload_rel_R_path.c_str(), std::ofstream::binary);
    //fp_R.open(params.workload_rel_R_path.c_str(), std::ios::out | std::ios::binary);
    int left_value_size = params.left_E_size - params.join_key_size;
    tmp_entry = std::string(left_value_size, '\0');
    while(start < keys.size()){
        end = start + tuples_per_page;
	if(end >= keys.size()) end = keys.size();
	memset(buffer, 0, DB_PAGE_SIZE);
	for(auto i = 0; i < end - start; i++){
	    for(auto j = 0; j < left_value_size; j++){
		tmp_entry[j] = value_alphanum[rand() % (sizeof(value_alphanum) - 1)];
	    }
            strcpy(buffer+i*(params.left_E_size), keys[start + i].c_str());
            strcpy(buffer+i*(params.left_E_size)+params.join_key_size, tmp_entry.c_str());
	}
	fp_R.write(buffer, DB_PAGE_SIZE);

	start = end;
    }
    fp_R.flush();
    fp_R.close();

}
