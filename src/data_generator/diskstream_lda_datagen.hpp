/*
 * diskstream_lda_datagen.hpp
 *
 *  Created on: Jul 12, 2013
 *      Author: jinliang
 */

#ifndef __DISKSTREAM_LDA_DATAGEN_HPP__
#define __DISKSTREAM_LDA_DATAGEN_HPP__

#include "../diskstream_buffermanager.hpp"
#include <string>
#include <boost/random/mersenne_twister.hpp>
#include <zmq.hpp>

#include "../diskstream_iothread.hpp"

namespace diskstream {

  class LdaDataGen {
  private:
    int32_t num_uwords;
    int32_t num_docs;
    int32_t max_word_freq; // maximum word frequency in one document
    int32_t max_uwords_per_doc;
    int32_t min_uwords_per_doc;


    BufferManager *lda_writer;
    BufferManager *edgelist_writer;
    zmq::context_t *zmq_ctx;
    DiskIOThread *diskio;
    int32_t max_line_len;

    std::string basename;
    std::string datadir;
    boost::mt19937 rng;

  public:
    LdaDataGen();
    ~LdaDataGen();

    int init(int32_t _num_uwords, int32_t _num_docs, int32_t _max_word_freq,
             int32_t _max_uwords_per_doc, int32_t _min_uwords_per_doc, std::string _basename,
             std::string _datadir, int32_t _max_line_len);

    int run();
  };
}




#endif /* DISKSTREAM_LDA_DATAGEN_HPP_ */
