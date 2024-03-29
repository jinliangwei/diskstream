#include "lda_program.hpp"
#include "lda_datastruct.hpp"

namespace diskstream {

  LdaSampler::LdaSampler(double _alpha, double _lambda, int32_t _rng_seed):
    alpha(_alpha),
    lambda(_lambda),
    num_tops(0),
    num_vocs(0),
    rng(_rng_seed),
    sum_topic_vec(NULL){}

  int LdaSampler::init(int32_t _num_topics, int32_t _num_vocs){
    num_tops = _num_topics;
    num_vocs = _num_vocs;

    probs.resize(num_tops);

    sum_topic_vec = new int32_t[num_tops];

    return 0;
  }

  int32_t LdaSampler::sample_random_topic(){
    return static_cast<int32_t>(rng.rand() * num_tops);
   }

   // Computes log(P(w|z,beta)) for one token
   double LdaSampler::compute_w_prob(int32_t word_topic_dist, int32_t sum_topic_dist) {
     return (word_topic_dist + lambda)/(sum_topic_dist + num_vocs*lambda);
   }


   //Gibbs samples a new value for the topic of a word.
   int32_t LdaSampler::sample_topic(const int32_t *doc_topic_vec,
                                    const int32_t *word_topic_vec, prob_mode_t probmode) {

     // Compute unnormalized prior/posterior probabilities for all topics 1 through K
     for (int32_t k = 0; k < num_tops; ++k) {
       probs[k] = doc_topic_vec[k] + alpha;
       if (probmode == POSTERIOR) {
         probs[k] *= compute_w_prob(word_topic_vec[k], sum_topic_vec[k]);
       }
     }
     // Sample a new value for topic
     return static_cast<int32_t>(rng.randDiscrete(probs, 0, num_tops));
   }

   int LdaSampler::add_one_topic_assign(int32_t top){

     ++sum_topic_vec[top];
     return 0;
   }

   int LdaSampler::replace_one_topic_assign(int32_t top, int32_t old_top){
     --sum_topic_vec[old_top];
     ++sum_topic_vec[top];
     return 0;
   }

   int LdaProgram::partial_reduce(uint8_t *task, uint8_t *data, uint8_t *globdata,
                                  int32_t niter, int32_t task_idx){
     Word *wd = (Word *) task;
     Document *doc = (Document *) data;
     LdaSampler *sampler = (LdaSampler *) globdata;
     if(niter == 0){
       int32_t wrank;
       int32_t size;
       const int32_t *wcnt_list = doc->get_wcnt_list(size);
       for(wrank = 0; wrank < wcnt_list[task_idx]; ++wrank){
         int32_t top = sampler->sample_random_topic();
         wd->add_topic_assign(top);
         doc->add_topic_assign(top, task_idx, wrank);
         sampler->add_one_topic_assign(top);
       }
     }else{
       const int32_t *doc_topic_vec = doc->get_topic_vec();
       const int32_t *word_topic_vec = wd->get_topic_vec();
       int32_t wrank;
       int32_t size;
       const int32_t *wcnt_list = doc->get_wcnt_list(size);
       for(wrank = 0; wrank < wcnt_list[task_idx]; ++wrank){
         int32_t top = sampler->sample_topic(doc_topic_vec, word_topic_vec);
         int32_t old_top = doc->replace_topic_assign(top, task_idx, wrank);
         wd->replace_topic_assign(top, old_top);
         sampler->replace_one_topic_assign(top, old_top);
       }
     }
     return 0;
   }
}
