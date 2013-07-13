/*
 * lda_program.hpp
 *
 *  Created on: Jul 13, 2013
 *      Author: jinliang
 */

#ifndef __LDA_PROGRAM_HPP__
#define __LDA_PROGRAM_HPP__

#include "random.hpp"

namespace diskstream {
  class DiskStreamProgram {
  private:

  public:
    DiskStreamProgram();
    ~DiskStreamProgram();

    int partial_reduce(uint8_t *task, uint8_t *data);

    int summarize(uint8_t *task);

    int end_iteration();

    int64_t get_cnt();
  };

  class LdaSampler{

  private:
      double alpha;
      double lambda;

      int32_t num_docs;
      int32_t num_tops;
      int32_t num_vocs;

      //random number generator
      Random rng;

      std::vector<double> probs;

      enum prob_mode_t {PRIOR, POSTERIOR}; // Specifies a probability type to compute
      int32_t *sum_topic_vec;
  public:
      LdaSampler(double _alpha, double _lambda, int32_t _rng_seed);

      int init(int32_t _num_docs, int32_t _num_topics, int32_t _num_vocs);
      int32_t sample_random_topic();
      double compute_w_prob(int32_t word_topic_dist, int32_t sum_topic_dist);
      int32_t sample_topic(int32_t *doc_topic_vec, int32_t wid,
          int32_t *word_topic_vec, prob_mode_t probmode=POSTERIOR);
      int add_one_topic_assign(int32_t top);
      int subtract_one_topic_assign(int32_t top);
  };
}


#endif /* LDA_PROGRAM_HPP_ */
