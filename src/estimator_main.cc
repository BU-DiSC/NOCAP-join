#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cmath>


#include "args.hxx"
#include "parameters.h"
#include "estimator.h"

int parse_arguments(int argc, char *argv[], Params & params);
void load_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params);

int main(int argc, char* argv[]) {
    Params params;
    if(parse_arguments(argc, argv, params)){
        exit(1);
    }
    std::vector<std::string> keys;
    std::vector<uint32_t> key_multiplicity;
    load_workload(keys, key_multiplicity, params);
    Estimator etm = Estimator(keys, key_multiplicity, params);
    uint64_t cost = etm.get_estimated_io() ;

    if(etm.repartitioned_keys[0] > 0){
	std::cout << "Available buffer for one partition: " << etm.left_entries_per_page*(params.B-2) << std::endl;
	std::cout << "Repartition cost: " << std::endl;
	uint64_t sum = 0U;
        for(int i = 0; i < etm.repartitioned_keys.size(); i++){
	    if(etm.repartitioned_keys[i] == 0) break;
	    std::cout << "Repartition depth " << i+1 << " : " << etm.repartitioned_keys[i] << std::endl;
	    sum += etm.repartitioned_keys[i];
	}	
	std::cout << "Total repartition cost : " << sum*params.io_latency << " microseconds" << std::endl;
    }else{
	std::cout << "Total repartition cost : 0" << " microseconds" << std::endl;
    }
    std::cout << cost << std::endl;
    std::cout << "CPU Time : " << etm.cpu_duration << " microseconds "<< std::endl;
    std::cout << "I/O Time : " << etm.io_duration << " microseconds " << std::endl;
    std::cout << "Estimated Time : " << (etm.cpu_duration + etm.io_duration)/1000000.0 << " seconds" << std::endl;
    return 0;
}


int parse_arguments(int argc, char *argv[], Params & params){
    args::ArgumentParser parser("partition_emul_parser", "");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
    args::Group partitioned_join_method_group(parser, "The is the group of partitioned join methods (PJMs):", args::Group::Validators::Xor);

    args::ValueFlag<uint32_t> buffer_size_cmd(group1, "B", "the buffer size in pages [def: 8192]", {'B',"buffer"});
    //args::ValueFlag<uint32_t> page_size_cmd(group1, "P", "the page size in bytes [def: 4096]", {'P',"page_size"});
    args::ValueFlag<double> c_cmd(group1, "c", "the constant factor to lower the threshold for MatrixDP in hash-matrixDP mode [def: log B]", {'c'});
    args::ValueFlag<double> th_cmd(group1, "th", "the threshold for hybrid partition mode [def: n^(m*m/n)/m ]", {"th"});
    args::ValueFlag<uint32_t> io_latency_cmd(group1, "IO", "the latency (in us) for 1 I/O [def: 100]", {"IO","io_latency"});

    args::ValueFlag<std::string> workload_path_dis_cmd(group1, "path", "the workload distribution path [def: ./workload-dis.txt]", {"path-dis"});
    args::ValueFlag<std::string> workload_path_rel_R_cmd(group1, "path", "the path for relation R [def: ./workload-rel-R.dat]", {"path-rel-R"});
    args::ValueFlag<std::string> workload_path_rel_S_cmd(group1, "path", "the path for relation S [def: ./workload-rel-S.dat]", {"path-rel-S"});
    args::Flag hash_plus_matrisDP_cmd(group1, "Apply MatrixDP after Hash with constant threshold c", "enable matrixDP after hash", {'H',"hybrid"});

    args::Flag hash_pjm_cmd(partitioned_join_method_group, "PJM-Hash", "Hash Partition for Joining", {"PJM-Hash"});
    args::Flag matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-MatrixDP", "Optimal Partition (produced by Matrix-DP) for Joining", {"PJM-MatrixDP"});
    args::Flag BNLJ_pjm_cmd(partitioned_join_method_group, "PJM-BNLJ", "Block Nested Loop Join (BNLJ)", {"PJM-BNLJ"});
    args::Flag hybrid_pjm_cmd(partitioned_join_method_group, "PJM-Hybrid", "Divide and partition (PJM-Hash + PJM-MatrixDP)", {"PJM-Hybrid"});
    args::ValueFlag<uint32_t> hash_type_pjm_cmd(group1, "PJM-Hash-Type", "The hash function type used in hash partitioned join [0: robin-hood, 1: crc32, 2: xxhash, 3:murmurhash, def: 0]", {"PJM-Hash-Function"});


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
     params.B = buffer_size_cmd ? args::get(buffer_size_cmd) : 8192;
     if(params.B < 3){
        std::cerr << "\033[0;31m Error: \033[0m The buffer size should be at least 3 pages." << std::endl;
	return 1;
     }
     //params.page_size = page_size_cmd ? args::get(page_size_cmd) : 4096;
     params.page_size = DB_PAGE_SIZE;
     params.io_latency = io_latency_cmd ? args::get(io_latency_cmd) : 100;
     params.c = c_cmd ? args::get(c_cmd) : 2*log(params.B);
     if(c_cmd){
	  params.c = args::get(c_cmd);
	  if(params.c <= 0){
              std::cerr << "\033[0;31m Error: \033[0m The constant factor c should be a positive number." << std::endl;
	      return 1;
	  }
     }else{
	  params.c = 0;
     }

     if(th_cmd){
	  params.th = args::get(th_cmd);
	  if(params.th < 0 || params.th > 1){
              std::cerr << "\033[0;31m Error: \033[0m The threshold should be a positive number between 0.0 and 1.0" << std::endl;
	      return 1;
	  }
     }else{
	  params.th = -1;
     }
     params.hybrid = hash_plus_matrisDP_cmd ? args::get(hash_plus_matrisDP_cmd) : false;


     params.workload_dis_path = workload_path_dis_cmd ? args::get(workload_path_dis_cmd) : "./workload-dis.txt";
     params.workload_rel_R_path = workload_path_rel_R_cmd ? args::get(workload_path_rel_R_cmd) : "./workload-rel-R.txt";
     params.workload_rel_S_path = workload_path_rel_S_cmd ? args::get(workload_path_rel_S_cmd) : "./workload-rel-S.txt";

     if(hash_pjm_cmd){
	params.pjm = Hash;
     }else if(matrixDP_pjm_cmd){
	params.pjm = MatrixDP;
     }else if(BNLJ_pjm_cmd){
	params.pjm = BNLJ;
     }else{
	params.pjm = Hybrid;
     }
     uint32_t hash_type_number = hash_type_pjm_cmd ? args::get(hash_type_pjm_cmd) : 0x0U;
     switch(hash_type_number){
         case 0x0U:
		 params.ht = RobinHood;
		 break;
         case 0x1U:
		 params.ht = CRC;
		 break;
	 case 0x2U:
		 params.ht = MurMurhash;
		 break;
         case 0x3U:
		 params.ht = XXhash;
		 break;
         case 0x4U:
		 params.ht = SHA2;
		 break;
	 case 0x5U:
		 params.ht = MD5;
		 break;
	 case 0x6U:
		 params.ht = CITY;
		 break;
         default:
		 params.ht = RobinHood;
                 std::cerr << "\033[0;31m Error: \033[0m The hash type parameter should be an integer between [0,6]" << std::endl;
		 return 1;
     }
     
     return 0;
}


void load_workload(std::vector<std::string> & keys, std::vector<uint32_t> & key_multiplicity, Params & params){
    std::ifstream fp;
    fp.open(params.workload_dis_path.c_str(), std::ios::in);
    //std::cout << "Using " << params.workload_path << std::endl;
    fp >> params.left_table_size >> params.right_table_size >> params.K >> params.left_E_size >> params.right_E_size;
    keys = std::vector<std::string> (params.left_table_size, "");
    key_multiplicity = std::vector<uint32_t> (params.left_table_size, 0);
    for(auto i = 0; i < params.left_table_size; i++){
	fp >> keys[i] >> key_multiplicity[i];
    }
    if(params.c == 0){
	params.c = log(log(params.left_table_size)) + 2*log(params.B) - log(log(params.B));
    }
    if(params.th == -1){
	params.th = pow(static_cast<double>(params.left_table_size) ,static_cast<double>(params.B*params.B)*1.0/params.left_table_size)/(params.B*1.0);
	if(params.th > 1) params.th = 1.0;
	std::cout << "th : " << params.th << std::endl;
    }
    fp.close();

}


