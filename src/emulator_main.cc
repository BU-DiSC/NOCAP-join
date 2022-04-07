#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cmath>
#include <unistd.h>

#include "args.hxx"
#include "parameters.h"
#include "emulator.h"

int parse_arguments(int argc, char *argv[], Params & params);
void load_workload(Params & params);
    

int main(int argc, char* argv[]) {
    Params params;
    if(parse_arguments(argc, argv, params)){
        exit(1);
    }
    Emulator emul = Emulator(params);
    emul.get_emulated_cost();
    std::cout << "Output #entries : " << emul.output_cnt << std::endl;
    std::cout << "Read #pages: " << emul.read_cnt << std::endl;
    std::cout << "Write #pages (excluding output): " << emul.write_cnt - emul.output_write_cnt << std::endl;
    std::cout << "Join Time : " << emul.join_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "I/O Time : " << emul.io_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "algo Time : " << emul.algo_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition CPU Time : " << emul.partition_cpu_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition IO Time : " << emul.partition_io_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition Time : " << emul.partition_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Probe Time : " << emul.probe_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Read Latency Per I/O : " << emul.tmp_duration/emul.read_cnt << " microseconds "<< std::endl;
    std::cout << "Write Latency Per I/O : " << (emul.io_duration - emul.tmp_duration)/emul.write_cnt << " microseconds "<< std::endl;
    return 0;
}


int parse_arguments(int argc, char *argv[], Params & params){
    args::ArgumentParser parser("partition_emul_parser", "");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
    args::Group partitioned_join_method_group(parser, "The is the group of partitioned join methods (PJMs):", args::Group::Validators::Xor);

    args::ValueFlag<uint32_t> buffer_size_cmd(group1, "B", "the buffer size in pages [def: 8192]", {'B',"buffer"});
    args::ValueFlag<uint32_t> num_of_partitions_cmd(group1, "NUM_Parts", "the number of partitions in Hash Join and Dynamic Hybrid Hash Join [def: B-1 for Grace Hash Join, 32 for Dynamic Hybrid Hash Join]", {"num_parts"});
    args::ValueFlag<uint32_t> k_cmd(group1, "k", " top k x (#entries per block) records to be tracked in MatrixDP [def:25]", {'k'});
    //args::ValueFlag<uint32_t> page_size_cmd(group1, "P", "the page size in bytes [def: 4096]", {'P',"page_size"});
    args::ValueFlag<double> randwrite_seqread_ratio_cmd(group1, "mu", "the threshold between random write and sequential read [def: 5 ]", {"mu"});

    args::ValueFlag<std::string> workload_path_dis_cmd(group1, "path", "the workload distribution path [def: ./workload-dis.txt]", {"path-dis"});
    args::ValueFlag<std::string> workload_path_rel_R_cmd(group1, "path", "the path for relation R [def: ./workload-rel-R.dat]", {"path-rel-R"});
    args::ValueFlag<std::string> workload_path_rel_S_cmd(group1, "path", "the path for relation S [def: ./workload-rel-S.dat]", {"path-rel-S"});
    args::ValueFlag<std::string> output_path_cmd(group1, "path", "the path for join output [def: ./join-output.dat]", {"path-output"});
    args::Flag rounded_hash_cmd(group1, "RoundedHash", " enable rounded hash in hash partitioned join", {"RoundedHash"});
    args::Flag debug_cmd(group1, "Debug", " enable debug mode to print more information", {"debug"});

    args::Flag hash_pjm_cmd(partitioned_join_method_group, "PJM-GHJ", "Grace Hash Partition for Joining", {"PJM-GHJ"});
    args::Flag sort_merge_join_pjm_cmd(partitioned_join_method_group, "PJM-SMJ", "Sort Merge for Joining", {"PJM-SMJ"});
    args::Flag dynamical_hybrid_hash_pjm_cmd(partitioned_join_method_group, "PJM-Dynamical-Hybrid-Hash", "Dynamic Hash Partition for Joining", {"PJM-DHH"});
    args::Flag matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-MatrixDP", "Optimal Partition (produced by Matrix-DP) for Joining", {"PJM-MatrixDP"});
    args::Flag approx_matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-ApprMatrixDP", "Approximated Partition (produced by Matrix-DP) for Joining", {"PJM-ApprMatrixDP"});
    args::Flag NBJ_pjm_cmd(partitioned_join_method_group, "PJM-NBJ", "Block Nested Loop Join (NBJ)", {"PJM-NBJ"});
    args::ValueFlag<uint32_t> hash_type_pjm_cmd(group1, "PJM-GHJ-Type", "The hash function type used in hash partitioned join or hybrid hash join [0: robin-hood, 1: crc32, 2: xxhash, 3:murmurhash, def: 0]", {"PJM-GHJ-Function"});


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

     params.workload_dis_path = workload_path_dis_cmd ? args::get(workload_path_dis_cmd) : "./workload-dis.txt";
     load_workload(params);


     params.B = buffer_size_cmd ? args::get(buffer_size_cmd) : 8192;
     if(params.B < 3){
        std::cerr << "\033[0;31m Error: \033[0m The buffer size should be at least 3 pages." << std::endl;
	return 1;
     }
     params.NBJ_outer_rel_buffer = 1;
     //params.page_size = page_size_cmd ? args::get(page_size_cmd) : 4096;
     params.page_size = DB_PAGE_SIZE;
     params.randwrite_seqread_ratio = randwrite_seqread_ratio_cmd ? args::get(randwrite_seqread_ratio_cmd) : 5;
     
     params.workload_rel_R_path = workload_path_rel_R_cmd ? args::get(workload_path_rel_R_cmd) : "./workload-rel-R.dat";
     params.workload_rel_S_path = workload_path_rel_S_cmd ? args::get(workload_path_rel_S_cmd) : "./workload-rel-S.dat";
     params.output_path = output_path_cmd ? args::get(output_path_cmd) : "./join-output.dat";

     uint32_t left_entries_per_page = floor(DB_PAGE_SIZE/params.left_E_size);	    
     uint32_t step_size = floor(left_entries_per_page*(params.B - 1 - params.NBJ_outer_rel_buffer)/FUDGE_FACTOR);
     uint32_t k = k_cmd ? args::get(k_cmd)*step_size : 25*step_size;
     params.k = k;
     if(hash_pjm_cmd){
	params.pjm = GHJ;
	if(num_of_partitions_cmd){
	    params.num_partitions = args::get(num_of_partitions_cmd);
	    if(params.num_partitions > params.B - 1){
                 std::cerr << "\033[0;31m Error: \033[0m The number of partitions should be no more than B-1 in hash join" << std::endl;
		 return 1;
	    }
	    std::cout << " Using Grace Hash Partitioned Join and #partitions is manually configured as " << params.num_partitions << "." << std::endl;
	}else{
	    uint32_t suggested_partitions = ceil(params.left_table_size/step_size);
	    if(suggested_partitions > params.B - 1){
	        params.num_partitions = params.B - 1;
	        std::cout << " Using Grace Hash Partitioned Join and #partitions is forced to be " << params.num_partitions << " due to the limited memory budget." << std::endl;
	    }else{
	        params.num_partitions = suggested_partitions;
	        std::cout << " Using Grace Hash Partitioned Join and #partitions is configured as the suggested one (" << params.num_partitions << ")." << std::endl;
	    }
	}
     }else if(matrixDP_pjm_cmd){
	params.num_partitions = params.B - 1;
	params.pjm = MatrixDP;
	std::cout << " Using MatrixDP-partitioned Join." << std::endl;
     }else if(NBJ_pjm_cmd){
	params.pjm = NBJ;
	std::cout << " Using Block Nexted Loop Join." << std::endl;
     }else if(dynamical_hybrid_hash_pjm_cmd){
	params.pjm = DynamicHybridHash;
        if(num_of_partitions_cmd){
	    params.num_partitions = args::get(num_of_partitions_cmd);
            if(params.num_partitions > params.B - 2){
                 std::cerr << "\033[0;31m Error: \033[0m The number of partitions should be no more than B-2 in dynamic hybrid hash join" << std::endl;
		 return 1;
	    }
	}else{
	    params.num_partitions = 32;
	}
	std::cout << " Using Dynamic Hybrid Hash Join and #partitions is configured as " << params.num_partitions << "." << std::endl;

     }else if(approx_matrixDP_pjm_cmd){
	params.num_partitions = params.B - 1;
	params.pjm = ApprMatrixDP;
	// find k max
	uint32_t start_k = floor(((params.B - 4)*step_size)/(1 + 1.0*step_size*FUDGE_FACTOR*(params.K + 2)/DB_PAGE_SIZE));
	while(true){
	    if(Emulator::get_hash_map_size(start_k+1, params.K) + ceil(k/step_size) > params.B - 2) break;
	    start_k++;
	}
	if(params.k > start_k){
	    std::cout << " Using Approximated MatrixDP-partitioned Join and k is forced to be k_max (" << start_k << ")." << std::endl;
	    params.k = start_k;
	}else{
	    std::cout << " Using Approximated MatrixDP-partitioned Join with k = " << params.k << "." << std::endl;
	}
     }else if(sort_merge_join_pjm_cmd){
	params.pjm = SMJ;
     }


     params.rounded_hash = rounded_hash_cmd ? args::get(rounded_hash_cmd) : false;
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
     params.SMJ_greater_flag = true; 
     params.debug = debug_cmd ? args::get(debug_cmd) : false; 
     return 0;
}


void load_workload(Params & params){
    std::ifstream fp;
    fp.open(params.workload_dis_path.c_str(), std::ios::in);
    //std::cout << "Using " << params.workload_path << std::endl;
    fp >> params.left_table_size >> params.right_table_size >> params.K >> params.left_E_size >> params.right_E_size;
    fp.close();

}


