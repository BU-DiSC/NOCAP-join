#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cmath>
#include <sys/stat.h>
#include <unistd.h>

#include "args.hxx"
#include "parameters.h"
#include "emulator.h"

int parse_arguments(int argc, char *argv[], Params & params);
void load_workload(Params & params);
int exist(const char *name)
{
    struct stat buffer;
    return (stat (name, &buffer) == 0);
}
    

int main(int argc, char* argv[]) {
    Params params;
    if(parse_arguments(argc, argv, params)){
        exit(1);
    }
    Emulator emul = Emulator(params);
    emul.get_emulated_cost();
    std::cout << "Output #entries : " << emul.output_cnt << std::endl;
    std::cout << "Total Read #pages: " << emul.read_cnt << std::endl;
    std::cout << "Total Write #pages (excluding output): " << emul.write_cnt - emul.output_write_cnt << std::endl;
    std::cout << "Sequential Write #pages (excluding output): " << emul.seq_write_cnt << std::endl;
    std::cout << "Normalized I/Os (excluding output): " << emul.read_cnt + (emul.write_cnt - emul.output_write_cnt - emul.seq_write_cnt)*params.randwrite_seqread_ratio + emul.seq_write_cnt*params.seqwrite_seqread_ratio << std::endl;
    std::cout << "Join Time : " << emul.join_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "I/O Time : " << emul.io_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "algo Time : " << emul.algo_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition CPU Time : " << emul.partition_cpu_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition IO Time : " << emul.partition_io_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Partition Time : " << emul.partition_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Probe Time : " << emul.probe_duration/1000000.0 << " seconds "<< std::endl;
    std::cout << "Read Latency Per I/O : " << emul.read_duration/emul.read_cnt << " microseconds "<< std::endl;
    if(emul.write_cnt == emul.output_write_cnt){
        std::cout << "Write Latency Per I/O (output) : " << emul.output_duration/emul.output_write_cnt << " microseconds "<< std::endl;
    }else{
        std::cout << "Write Latency Per I/O (excluding output): " << (emul.io_duration - emul.read_duration - emul.output_duration)/(emul.write_cnt - emul.output_write_cnt) << " microseconds "<< std::endl;
    }
    return 0;
}


int parse_arguments(int argc, char *argv[], Params & params){
    args::ArgumentParser parser("partition_emul_parser", "");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
    args::Group partitioned_join_method_group(parser, "The is the group of partitioned join methods (PJMs):", args::Group::Validators::Xor);

    args::ValueFlag<uint32_t> buffer_size_cmd(group1, "B", "the buffer size in pages [def: 8192]", {'B',"buffer"});
    args::ValueFlag<uint32_t> num_of_partitions_cmd(group1, "NUM_Parts", "the number of partitions in Hash Join and Dynamic Hybrid Hash Join [def: B-1 for Grace Hash Join, 32 for Dynamic Hybrid Hash Join]", {"num_parts"});
    args::ValueFlag<uint32_t> k_cmd(group1, "k", " top k records to be tracked in MatrixDP [def:min(relation R size, 50000)]", {'k'});
    //args::ValueFlag<uint32_t> page_size_cmd(group1, "P", "the page size in bytes [def: 4096]", {'P',"page_size"});
    args::ValueFlag<double> randwrite_seqread_ratio_cmd(group1, "mu", "the threshold between random write and sequential read [def: 5 ]", {"mu"});
    args::ValueFlag<double> seqwrite_seqread_ratio_cmd(group1, "tau", "the threshold between sequential write and sequential read [def: 3.5 ]", {"tau"});
    args::ValueFlag<double> hashtable_fulfilling_percent_cmd(group1, "alpha", "the full-filling percent threshold in rounded hash [def: 0.95 ]", {"alpha"});
    args::ValueFlag<double> left_selection_ratio_cmd(group1, "lSR", "the selection ratio from left table [default: 1.0]", {"lSR"});
    args::ValueFlag<double> right_selection_ratio_cmd(group1, "rSR", "the selection ratio from right table [default: 1.0]", {"rSR"});
    args::ValueFlag<uint64_t> left_selection_seed_cmd(group1, "lSS", "the selection seed for the left table [default: a random value]", {"lSS"});
    args::ValueFlag<uint64_t> right_selection_seed_cmd(group1, "rSS", "the selection seed for the right table [default: a random value]", {"rSS"});
    args::ValueFlag<double> DHH_skew_partition_percent_cmd(group1, "DHH_skew_percent", "the proportion of assigned memory for a skewed in-memory partition [default: 0.02]", {"DHH_skew_mem_percent"});
    args::ValueFlag<double> DHH_skew_off_threshold_cmd(group1, "DHH_skew_off_threshold", "the lower bound of the fraction between prioritized in-memory skew entries and the outer relation entries [default: 0.01]", {"DHH_skew_frac_threshold"});

    args::ValueFlag<std::string> workload_path_dis_cmd(group1, "path", "the workload distribution path [def: ./workload-dis.txt]", {"path-dis"});
    args::ValueFlag<std::string> workload_path_rel_R_cmd(group1, "path", "the path for relation R [def: ./workload-rel-R.dat]", {"path-rel-R"});
    args::ValueFlag<std::string> workload_path_rel_S_cmd(group1, "path", "the path for relation S [def: ./workload-rel-S.dat]", {"path-rel-S"});
    args::ValueFlag<std::string> output_path_cmd(group1, "path", "the path for join output [def: ./join-output.dat]", {"path-output"});
    args::ValueFlag<std::string> part_stats_path_cmd(group1, "path", "the path for partition statistics collection (CT,partition size) [def: ./part-stats.txt]", {"stats-path-output"});
    args::Flag rounded_hash_cmd(group1, "RoundedHash", " enable rounded hash in hash partitioned join", {"RoundedHash"});
    args::Flag no_direct_io_cmd(group1, "DisableDirectIO", " disable direct I/O ", {"NoDirectIO"});
    args::Flag no_sync_io_cmd(group1, "DisableSyncIO", " disable sync I/O ", {"NoSyncIO"});
    args::Flag no_join_output_cmd(group1, "DisableJoinOutput", " no join output (for estimation) ", {"NoJoinOutput"});
    args::Flag no_smj_recursive_join_cmd(group1, "DisableSMJRecursiveJoin", " disable SMJ in partitioned join", {"NoSMJPartWiseJoin"});
    args::Flag clct_part_stats_only_cmd(group1, "CollectPartitionStatsOnly", " only collect the partition statistics for partitioning (for statistics collection, nothing to print for SMJ and NBJ, minor optimization for rounded hash is not implemented yet) ", {"ClctPartStatsOnly"});
    args::Flag debug_cmd(group1, "Debug", " enable debug mode to print more information", {"debug"});
    args::Flag tpch_q12_flag_cmd(group1, "TPCH-Q12-Flag", " emulate the Q12 in tpc-h dataset", {"tpch-q12"});
    args::ValueFlag<std::string> tpch_q12_path_cmd(group1, "TPCH-Q12-Path", " the path for Q12 query generated by tpch", {"tpch-q12-path"});

    args::Flag hash_pjm_cmd(partitioned_join_method_group, "PJM-GHJ", "Grace Hash Partition for Joining", {"PJM-GHJ"});
    args::Flag sort_merge_join_pjm_cmd(partitioned_join_method_group, "PJM-SMJ", "Sort Merge for Joining", {"PJM-SMJ"});
    args::Flag dynamical_hybrid_hash_pjm_cmd(partitioned_join_method_group, "PJM-Dynamical-Hybrid-Hash", "Dynamic Hash Partition for Joining", {"PJM-DHH"});
    args::Flag matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-MatrixDP", "Optimal Partition OCAP without Hybrid Join (produced by Matrix-DP)", {"PJM-MatrixDP"});
    args::Flag hybrid_matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-HybridMatrixDP", "Optimal Partition OCAP with Hybrid Join (produced by Matrix-DP considering Hybrid Hash Join)", {"PJM-HybridMatrixDP"});
    args::Flag approx_matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-ApprMatrixDP", "Approximated Partition NOCAP without Hybrid Join", {"PJM-ApprMatrixDP"});
    args::Flag approx_hybrid_matrixDP_pjm_cmd(partitioned_join_method_group, "PJM-ApprHybridMatrixDP", "Approximated Partition NOCAP with Hybrid Join", {"PJM-HybridApprMatrixDP"});
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
    if(!exist(params.workload_dis_path.c_str())) {
	std::cerr << "The workload distribution file path (" << params.workload_dis_path.c_str() << ") does not exist!" << std::endl;
	return 1;
    }
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
    params.hashtable_fulfilling_percent = hashtable_fulfilling_percent_cmd ? args::get(hashtable_fulfilling_percent_cmd) : 0.95;
    if(params.hashtable_fulfilling_percent < 0.0 || params.hashtable_fulfilling_percent > 1.0){
	    std::cout << "The full-filling percentage in rounded hash should be in the range [0.0,1.] (ideally, it should be a number close to 1.0 (e.g. 0.95) )" << std::endl;
    }
    params.seqwrite_seqread_ratio = seqwrite_seqread_ratio_cmd ? args::get(seqwrite_seqread_ratio_cmd) : 3.5;
    params.left_selection_ratio = left_selection_ratio_cmd ? args::get(left_selection_ratio_cmd) : 1.0;
    params.right_selection_ratio = right_selection_ratio_cmd ? args::get(right_selection_ratio_cmd) : 1.0;
    params.left_selection_seed = left_selection_seed_cmd ? args::get(left_selection_seed_cmd) : rand();
    params.right_selection_seed = right_selection_seed_cmd ? args::get(right_selection_seed_cmd) : rand();
    params.no_smj_partition_wise_join = no_smj_recursive_join_cmd ? args::get(no_smj_recursive_join_cmd) : false;
    params.DHH_skew_partition_percent = DHH_skew_partition_percent_cmd ? args::get(DHH_skew_partition_percent_cmd) : 0.02;
    params.DHH_skew_frac_threshold = DHH_skew_off_threshold_cmd ? args::get(DHH_skew_off_threshold_cmd) : 0.01;
    
    params.workload_rel_R_path = workload_path_rel_R_cmd ? args::get(workload_path_rel_R_cmd) : "./workload-rel-R.dat";

    if(!exist(params.workload_rel_R_path.c_str())) {
	std::cerr << "The left-side relation path (" << params.workload_rel_R_path.c_str() << ") does not exist!" << std::endl;
	return 1;
    }
    params.workload_rel_S_path = workload_path_rel_S_cmd ? args::get(workload_path_rel_S_cmd) : "./workload-rel-S.dat";
    if(!exist(params.workload_rel_S_path.c_str())) {
	std::cerr << "The right-side relation path (" << params.workload_rel_S_path.c_str() << ") does not exist!" << std::endl;
	return 1;
    }
    params.output_path = output_path_cmd ? args::get(output_path_cmd) : "./join-output.dat";
    params.clct_part_stats_only_flag = clct_part_stats_only_cmd ? args::get(clct_part_stats_only_cmd) : false;
    if(params.clct_part_stats_only_flag){
	    std::cout << "\033[36m Warning \033[0m : Partition stats collection mode is enabled. Not executing the join algorithm." << std::endl;
    }
    params.part_stats_path = part_stats_path_cmd ? args::get(part_stats_path_cmd) : "./part-stats.txt";
    params.hybrid = false; // by default, the hybrid mode is turned off
    uint32_t left_entries_per_page = floor(DB_PAGE_SIZE/params.left_E_size);	    
    uint32_t step_size = floor(left_entries_per_page*(params.B - 1 - params.NBJ_outer_rel_buffer)*1.0/FUDGE_FACTOR);
    //std::cout << "step size: " << step_size << std::endl;
    uint32_t k = k_cmd ? args::get(k_cmd) : 50000;
    if(k > params.left_table_size){
	    k = params.left_table_size;
    }
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
	        uint32_t suggested_partitions = ceil(params.left_table_size/(step_size*params.hashtable_fulfilling_percent));
	        if(suggested_partitions > params.B - 1){
	            params.num_partitions = params.B - 1;
	            std::cout << " Using Grace Hash Partitioned Join and #partitions is forced to be " << params.num_partitions << " due to the limited memory budget." << std::endl;
	        }else{
	            params.num_partitions = suggested_partitions;
	            std::cout << " Using Grace Hash Partitioned Join and #partitions is configured as the suggested one (" << params.num_partitions << ")." << std::endl;
	        }
	    }
    }else if(matrixDP_pjm_cmd || hybrid_matrixDP_pjm_cmd){
	    if (hybrid_matrixDP_pjm_cmd) {
		params.hybrid = true;
	        std::cout << " Using MatrixDP-partitioned Join (OCAP) with enabling Hybrid Join." << std::endl;
	    } else {
	        std::cout << " Using MatrixDP-partitioned Join (OCAP) with disabling Hybrid Join." << std::endl;
	    }
	    params.num_partitions = params.B - 1; // may be overwritten in get_partitioned_keys
	    params.pjm = MatrixDP;
        }else if(NBJ_pjm_cmd){
	        params.pjm = NBJ;
	        std::cout << " Using Block Nested Loop Join." << std::endl;
	        if(params.clct_part_stats_only_flag){
	            std::cout << "\033[36m Warning \033[0m : Collecting statistics for partitioning is enabled but not working for this join algorithm." << std::endl;
	        }
        }else if(dynamical_hybrid_hash_pjm_cmd){
	        params.pjm = DynamicHybridHash;
            if(num_of_partitions_cmd){
	            params.num_partitions = args::get(num_of_partitions_cmd);
                if(params.num_partitions > params.B - 2){
                    std::cerr << "\033[0;31m Error: \033[0m The number of partitions should be no more than B-2 in dynamic hybrid hash join" << std::endl;
		            return 1;
	            }
	        }else{
                // PostgreSQL ensures at least one partition fits in memory, so we calculate if there exists such a partition number so that
                // one large partition can fit in memory. Otherwise, we simply uses B - 2.
                uint32_t num_skew_in_memory_entries = 0;
	            uint32_t pages_for_in_memory_skew_partition = 0;
	            if (params.DHH_skew_partition_percent > 0) {
		            num_skew_in_memory_entries = std::min((uint32_t)floor(params.B*DB_PAGE_SIZE*params.DHH_skew_partition_percent/FUDGE_FACTOR/params.left_E_size), params.k);
		            pages_for_in_memory_skew_partition = (uint32_t)ceil((1.0*num_skew_in_memory_entries*params.left_E_size*FUDGE_FACTOR)/DB_PAGE_SIZE);
                    params.k = num_skew_in_memory_entries;
	            }
                
                double remaining_R_in_pages = ceil((params.left_table_size*params.left_selection_ratio - num_skew_in_memory_entries)*params.left_E_size*1.0/DB_PAGE_SIZE);
                if (remaining_R_in_pages*FUDGE_FACTOR > params.B) {
                    params.num_partitions = std::max(20U, (uint32_t)ceil((remaining_R_in_pages*FUDGE_FACTOR - params.B)/(params.B - 1)));
		    while (ceil(remaining_R_in_pages*FUDGE_FACTOR*1.0/params.num_partitions) + 2 > params.B*params.hashtable_fulfilling_percent) {
		        params.num_partitions++;
		    }
		    params.num_partitions = std::min(params.B - 1, params.num_partitions);
                    if (params.num_partitions < params.B - 1) {
                        std::cout << " Using Dynamic Hybrid Hash Join and #partitions is configured as " << params.num_partitions << "." << std::endl;
                    } else {
                        std::cout << " Dynamic Hybrid Hash Join may downgrade to Grace Hash Join and #partitions is configured as " << params.B - 1 << "." << std::endl;
                    }
		    
                    
                } else {
                    params.pjm = NBJ;
                }
                
	        }
	        

	        if(params.clct_part_stats_only_flag){
	            std::cout << "\033[36m Warning \033[0m : Collecting statistics for partitioning is enabled but not working for this join algorithm." << std::endl;
	        }
        }else if(approx_matrixDP_pjm_cmd || approx_hybrid_matrixDP_pjm_cmd){
	        params.num_partitions = params.B - 1; // may be overwritten in get_partitioned_keys
	        params.pjm = ApprMatrixDP;
	        // find k max
	        uint32_t start_k = floor(((params.B - 4)*step_size)/(1 + 1.0*step_size*FUDGE_FACTOR*(params.join_key_size + 2)/DB_PAGE_SIZE));
	        while(start_k < params.k){
	            if(Emulator::get_hash_map_size(start_k+1, params.join_key_size) + ceil(k/step_size) > params.B - 2) break;
	            start_k++;
	        }
	        if(params.k > start_k){

	            if (approx_hybrid_matrixDP_pjm_cmd) {
		        params.hybrid = true;
	                std::cout << " Using Approximated MatrixDP-partitioned Join (NOCAP) with enabling Hybrid Join and k is forced to be k_max (" << start_k << ")." << std::endl;
		    } else {
	                std::cout << " Using Approximated MatrixDP-partitioned Join (NOCAP) with disabling Hybrid Join and k is forced to be k_max (" << start_k << ")." << std::endl;
		    }
	            params.k = start_k;
	        }else{
	            if (approx_hybrid_matrixDP_pjm_cmd) {
		        params.hybrid = true;
	                std::cout << " Using Approximated MatrixDP-partitioned Join (NOCAP) with enabling Hybrid Join with k = " << params.k << "." << std::endl;
		    } else {
	                std::cout << " Using Approximated MatrixDP-partitioned Join (NOCAP) with disabling Hybrid Join with k = " << params.k << "." << std::endl;
		    }
	        }
        }else if(sort_merge_join_pjm_cmd){
	params.pjm = SMJ;
	if(params.clct_part_stats_only_flag){
	    std::cout << "\033[36m Warning \033[0m : Collecting statistics for partitioning is enabled but not working for this join algorithm." << std::endl;
	}
     }

     params.tpch_q12_flag = tpch_q12_flag_cmd ? args::get(tpch_q12_flag_cmd) : false;
     params.tpch_q12_path = tpch_q12_path_cmd ? args::get(tpch_q12_path_cmd) : "./Q12.sql";

    if(params.tpch_q12_flag && !exist(params.tpch_q12_path.c_str())) {
	std::cerr << "The TPCH Q12 sql path (" << params.tpch_q12_path.c_str() << ") does not exist!" << std::endl;
	return 1;
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
     params.no_direct_io = no_direct_io_cmd ? args::get(no_direct_io_cmd) : false;
     params.no_sync_io = no_sync_io_cmd ? args::get(no_sync_io_cmd) : false;
     params.no_join_output = no_join_output_cmd ? args::get(no_join_output_cmd) : false;
     params.debug = debug_cmd ? args::get(debug_cmd) : false; 
     
     return 0;
}


void load_workload(Params & params){
    std::ifstream fp;
    fp.open(params.workload_dis_path.c_str(), std::ios::in);
    //std::cout << "Using " << params.workload_path << std::endl;
    fp >> params.left_table_size >> params.right_table_size >> params.join_key_type >> params.join_key_size >> params.left_E_size >> params.right_E_size;
    fp.close();

}


