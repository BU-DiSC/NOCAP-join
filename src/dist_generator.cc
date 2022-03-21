#include "dist_generator.h"
#include "math.h"
#include <algorithm>
#include "time.h"

int myrandom (int i) { return std::rand()%i;}

DistGenerator::DistGenerator(){}
DistGenerator::DistGenerator(int dist, uint32_t size, double norm_stddev, double beta_alpha, double beta_beta, double zipf_alpha): dist_(dist), size_(size), norm_stddev_(norm_stddev), beta_alpha_(beta_alpha), beta_beta_(beta_beta), zipf_alpha_(zipf_alpha){
	gen.seed(std::chrono::system_clock::now().time_since_epoch().count());
	distribution0 = std::uniform_int_distribution<int>(0, size_-1);
	distribution1 = std::normal_distribution<double>(size_*0.5, norm_stddev_*size_*0.25);
	distribution2_x = std::gamma_distribution<double>(beta_alpha_, 1.0);
	distribution2_y = std::gamma_distribution<double>(beta_beta_, 1.0);


        //for zipfian distribution
        if(dist == 3){
            zipf_normalize_constant = 0;
            for(int i = 1; i <= size_; i++){
                zipf_normalize_constant += 1/(pow((double)i, zipf_alpha_));
            }
	    
            cumulative_probabilities = std::vector<double> ( size_+1, 0.0);
            cumulative_probabilities[0] = 0.0;
            for(int i = 1; i <= size_; i++){
                cumulative_probabilities[i] = cumulative_probabilities[i-1] + 1/(pow((double)i, zipf_alpha_)*zipf_normalize_constant);
            }
            cumulative_probabilities[size_] = 1.0;
            //std::random_shuffle(index_mapping.begin(), index_mapping.end(), myrandom); 
        }else{
            cumulative_probabilities = std::vector<double> ();
        }
        uniform_standard_distribution = std::uniform_real_distribution<double>(0.0,1.0);
}

uint32_t DistGenerator::getNext(){
	switch (dist_) { // 0 -> uniform; 1 -> norm; 2 -> beta; 3-> Zipf
		case 0: {
			return (uint32_t) distribution0(gen);
		}
		case 1: {
			uint32_t number = (uint32_t) round(distribution1(gen));
			while(number < 0 || number > size_-1){
				number = (uint32_t) round(distribution1(gen));
			}	
			return number;
		}
		case 2: {
			double X = distribution2_x(gen);
			double Y = distribution2_y(gen);
			
			return round(size_ * (X/(X+ Y)));
		}
                case 3: {
                        double p = uniform_standard_distribution(gen);
                        while(p == 0 || p == 1) p = uniform_standard_distribution(gen);
                        // binary search
                        int low = 0;
                        int high = size_-1;
                        int mid = (low + high)/2;
                        while(high - low > 1){
                            //std::cout << "p: " << p << "\tmid: " << cumulative_probabilities[mid] << "\tmid+1: " << cumulative_probabilities[mid+1] << std::endl;
                            if(p < cumulative_probabilities[mid]){
                                high = mid;
                            }else if(p >= cumulative_probabilities[mid+1]){
                                low = mid; 
                            }else{
                                break;
                            }
                            mid = (low+high)/2;
                        }
                        return mid; 

                }
		default:{
			std::cout << "Unexpected case" << std::endl;
			return 0;
		}

	}
}
