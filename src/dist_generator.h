#ifndef DIST_GENERATOR_H
#define DIST_GENERATOR_H

#include <random>
#include <iostream>
#include <chrono>
#include <vector>

class DistGenerator{
	int dist_; // 0 -> unifrom; 1 -> norm; 2 -> beta; 3 -> Zipf
	int size_;
	double norm_stddev_;
	double beta_alpha_;
	double beta_beta_;	
        double zipf_alpha_;
        double zipf_normalize_constant;
	std::default_random_engine gen;
	std::uniform_int_distribution<int> distribution0;
	std::normal_distribution<double> distribution1;
	std::gamma_distribution<double> distribution2_x;
	std::gamma_distribution<double> distribution2_y;

        // for zipf distribution
	std::uniform_real_distribution<double> uniform_standard_distribution; 
        std::vector<double> cumulative_probabilities;
	
public:
	DistGenerator();
	DistGenerator(int dist, uint32_t size, double norm_stddev, double beta_alpha, double beta_beta, double zipf_alpha);
	uint32_t getNext();
};
#endif
