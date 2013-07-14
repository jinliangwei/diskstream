/*
 * lda_program.hpp
 *
 *  Created on: Jul 13, 2013
 *      Author: jinliang
 */

#ifndef __LDA_PROGRAM_HPP__
#define __LDA_PROGRAM_HPP__

#include "random.hpp"
#include "diskstream_program.hpp"

namespace diskstream {
  class LdaProgram : public DiskStreamProgram{
  private:

  public:

    int partial_reduce(uint8_t *task, uint8_t *data, uint8_t *globadata,
                       int32_t niter, int32_t task_idx);

    //int summarize(uint8_t *task);

    //int end_iteration();

  };

  class LdaSampler{

  private:
      double alpha;
      double lambda;

      int32_t num_tops;
      int32_t num_vocs;

      //random number generator
      Random rng;

      std::vector<double> probs;

      enum prob_mode_t {PRIOR, POSTERIOR}; // Specifies a probability type to compute
      int32_t *sum_topic_vec;
  public:
      LdaSampler(double _alpha, double _lambda, int32_t _rng_seed);

      int init(int32_t _num_topics, int32_t _num_vocs);
      int32_t sample_random_topic();
      double compute_w_prob(int32_t word_topic_dist, int32_t sum_topic_dist);
      int32_t sample_topic(const int32_t *doc_topic_vec,
                           const int32_t *word_topic_vec, prob_mode_t probmode=POSTERIOR);
      int add_one_topic_assign(int32_t top);
      int replace_one_topic_assign(int32_t top, int32_t old_top);
  };
}


#endif /* LDA_PROGRAM_HPP_ */
