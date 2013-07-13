
#include "diskstream_lda_datagen.hpp"
#include "../diskstream_buffer.hpp"
#include "../util/diskstream_util.hpp"
#include <string.h>
#include <assert.h>
#include <list>

namespace diskstream {
  static const std::string KDataGenDiskIOEndp = "inproc://ldaexecutor_diskio_endp";
  const int32_t KBufferSize = 64;
  const int64_t KOneMega = 1024*1024;
  const int32_t KNumBuffs = 4;

  LdaDataGen::LdaDataGen():
    num_uwords(0),
    num_docs(0),
    max_word_freq(0),
    max_uwords_per_doc(0),
    min_uwords_per_doc(0),
    lda_writer(NULL),
    edgelist_writer(NULL),
    zmq_ctx(NULL),
    diskio(NULL),
    max_line_len(0){}

  LdaDataGen::~LdaDataGen(){
    if(lda_writer != NULL){
      delete lda_writer;
      lda_writer = NULL;
    }
    if(edgelist_writer != NULL){
      delete edgelist_writer;
      edgelist_writer = NULL;
    }
    if(diskio != NULL){
      diskio->stop();
      delete diskio;
      diskio = NULL;
    }
  }

  int LdaDataGen::init(int32_t _num_uwords, int32_t _num_docs, int32_t _max_word_freq,
             int32_t _max_uwords_per_doc, int32_t _min_uwords_per_doc, std::string _basename,
             std::string _datadir, int32_t _max_line_len){

    num_uwords = _num_uwords;
    num_docs = _num_docs;
    max_word_freq = _max_word_freq;
    max_uwords_per_doc = _max_uwords_per_doc;
    min_uwords_per_doc = _min_uwords_per_doc;
    basename = _basename;
    datadir = _datadir;
    max_line_len = _max_line_len;

    if(max_uwords_per_doc < min_uwords_per_doc) return -1;
    std::cout << "num_uwords = " << num_uwords << std::endl;
    try{
      zmq_ctx = new zmq::context_t(1);
      diskio = new DiskIOThread(zmq_ctx, 2, KDataGenDiskIOEndp.c_str(), datadir);
    }catch(...){
      return -1;
    }

    int suc = diskio->init();
    if(suc < 0) return -1;
    suc = diskio->start();
    if(suc < 0) return -1;

    try{
      lda_writer = new BufferManager(KNumBuffs*KBufferSize*KOneMega, KBufferSize*KOneMega, diskio);
      edgelist_writer = new BufferManager(KNumBuffs*KBufferSize*KOneMega, KBufferSize*KOneMega, diskio);
    }catch(...){
      return -1;
    }
    return 0;
  }

  int LdaDataGen::run(){
    char *templine_lda = new char[max_line_len], *templine_edgelist = new char[max_line_len];
    bool *uword_id_alloc = new bool[num_uwords];
    MemChunkAccesser lda_buff_acc, edgelist_buff_acc;
    int suc;
    suc = lda_writer->init_write_extern(datadir + "/" + basename + ".lda");
    assert(suc == 0);
    suc = edgelist_writer->init_write_extern(datadir + "/" + basename + ".edgelist");
    assert(suc == 0);
    Buffer *lda_buff = lda_writer->get_buffer(), *edgelist_buff = edgelist_writer->get_buffer();
    assert(lda_buff != NULL);
    assert(edgelist_buff != NULL);

    lda_buff_acc.set_mem(lda_buff->get_db_ptr(), lda_buff->get_db_size());
    std::cout << "lda_buff->get_db_size() = " << lda_buff->get_db_size() << std::endl;
    std::cout << "lda_buff_acc.avai_size() = " << lda_buff_acc.avai_size() << std::endl;
    edgelist_buff_acc.set_mem(edgelist_buff->get_db_ptr(), edgelist_buff->get_db_size());

    int32_t curr_doc_id;
    int32_t uwords_per_doc_diff = max_uwords_per_doc - min_uwords_per_doc + 1;

    std::list<int32_t> wid_list;
    for(curr_doc_id = 0; curr_doc_id < num_docs; ++curr_doc_id){
      if(curr_doc_id%5000 == 0){
        std::cout << "created " << curr_doc_id << " docs" << std::endl;
      }
      //std::cout << "curr_doc_id = " << curr_doc_id << std::endl;
      int32_t num_uwords_this_doc = min_uwords_per_doc + rng()%uwords_per_doc_diff;
      memset(uword_id_alloc, 0, sizeof(bool)*num_uwords);
      sprintf(templine_lda, "%d", num_uwords_this_doc);
      memset(templine_edgelist, 0, sizeof(char)*max_line_len);
      int32_t word_idx;
      for(word_idx = 0; word_idx < num_uwords_this_doc; ++word_idx){
        int32_t wid = rng()%num_uwords;
        while(uword_id_alloc[wid]){
          wid = rng()%num_uwords;
        }
        uword_id_alloc[wid] = true;
        wid_list.push_back(wid);
      }
      wid_list.sort();
      std::list<int32_t>::iterator it = wid_list.begin();
      for(it = wid_list.begin(); it != wid_list.end();++it){
        int32_t wfreq = rng()%(max_word_freq + 1);
        sprintf(templine_lda + strlen(templine_lda), " %d:%d", *it, wfreq);
        sprintf(templine_edgelist + strlen(templine_edgelist), "%d %d %d\n",
              *it, curr_doc_id, wfreq);
      }
      wid_list.clear();
      int32_t len = strlen(templine_lda);
      templine_lda[len] = '\n';
      ++len;
      if(lda_buff_acc.avai_size() < len){
        lda_buff->set_valid_db_size(lda_buff->get_db_size() - lda_buff_acc.avai_size());
        lda_writer->putback(lda_buff, BufPolicyWriteBack);
        lda_buff = lda_writer->get_buffer();
        assert(lda_buff != NULL);
        lda_buff_acc.set_mem(lda_buff->get_db_ptr(), lda_buff->get_db_size());
      }
      memcpy(lda_buff_acc.curr_ptr(), templine_lda, len);
      suc = lda_buff_acc.advance(len);
      assert(suc == 0);
      len = strlen(templine_edgelist);
      if(edgelist_buff_acc.avai_size() < len){
        edgelist_buff->set_valid_db_size(edgelist_buff->get_db_size() - edgelist_buff_acc.avai_size());
        edgelist_writer->putback(edgelist_buff, BufPolicyWriteBack);
        edgelist_buff = edgelist_writer->get_buffer();
        assert(edgelist_buff != NULL);
        edgelist_buff_acc.set_mem(edgelist_buff->get_db_ptr(), edgelist_buff->get_db_size());
      }
      memcpy(edgelist_buff_acc.curr_ptr(), templine_edgelist, len);
      suc = edgelist_buff_acc.advance(len);
      assert(suc == 0);
    }
    lda_buff->set_valid_db_size(lda_buff->get_db_size() - lda_buff_acc.avai_size());
    lda_writer->putback(lda_buff, BufPolicyWriteBack);
    edgelist_buff->set_valid_db_size(edgelist_buff->get_db_size() - edgelist_buff_acc.avai_size());
    edgelist_writer->putback(edgelist_buff, BufPolicyWriteBack);
    delete[] templine_lda;
    delete[] templine_edgelist;
    delete[] uword_id_alloc;

    lda_writer->stop();
    edgelist_writer->stop();
    return 0;
  }
}
