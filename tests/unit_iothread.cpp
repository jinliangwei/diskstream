#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <sys/time.h>
#include <fstream>
#include <iostream>
#include <string>
#include <stdint.h>
#include <zmq.hpp>
#include <assert.h>
#include <string.h>
#include <new>
#include <sys/stat.h>
#include <unistd.h>

#include <diskstream_buffer.hpp>
#include <diskstream_iothread.hpp>
#include <diskstream_msgtypes.hpp>
#include <util/diskstream_util.hpp>

namespace diskstream_unit {
  const int32_t KOneMega = (1024*1024);
  class Document {
  private:
    uint32_t num_words;
    uint32_t num_topics;
    uint32_t num_uwords; //number of unique words
    uint32_t word_array_offset;
    uint32_t word_cnt_offset;
    uint32_t topic_assign_offset;
    uint32_t topic_vec_offset;

    int32_t *temp_word_list;
    int32_t *temp_wcnt_list;
  public:
    Document():
      num_words(0),
      num_topics(0),
      num_uwords(0),
      word_array_offset(0),
      word_cnt_offset(0),
      topic_assign_offset(0),
      topic_vec_offset(0),
      temp_word_list(NULL),
      temp_wcnt_list(NULL){}

    ~Document(){
      if(temp_word_list != NULL) delete[] temp_word_list;
      if(temp_wcnt_list != NULL) delete[] temp_wcnt_list;
    }

    uint32_t size(){
      return sizeof(Document) + 2*num_uwords*sizeof(int32_t) + num_words*sizeof(int32_t)
                              + num_topics*sizeof(int32_t);
    }

    int pre_init(std::string text, int32_t _num_topics){
      num_topics = _num_topics;
      std::stringstream ss;
      ss << text;
      int32_t nuw, nw = 0, i = 0;
      ss >> nuw;
      if(nuw == 0) return -1;
      temp_word_list = new int32_t[nuw];
      temp_wcnt_list = new int32_t[nuw];
      while(ss.good()){
        int32_t wid, cnt;
        char c;
        ss >> wid >> c >> cnt;
        temp_word_list[i] = wid;
        temp_wcnt_list[i] = cnt;
        ++i;
        nw += cnt;
      }
      num_words = nw;
      num_uwords = nuw;
      return 0;
    }

    /* Sufficient amount of memory must be allocated in advance immediately following this object.
     * */
    int init(){
      word_array_offset = sizeof(Document);
      memcpy((uint8_t *) this + word_array_offset, temp_word_list, num_uwords*sizeof(int32_t));

      word_cnt_offset = word_array_offset + sizeof(int32_t)*num_uwords;
      memcpy((uint8_t *) this + word_cnt_offset, temp_wcnt_list, num_uwords*sizeof(int32_t));

      topic_assign_offset = word_cnt_offset + sizeof(int32_t)*num_uwords;
      memset((uint8_t *) this + topic_assign_offset, 0 , num_words*sizeof(int32_t));

      topic_vec_offset = topic_assign_offset + sizeof(int32_t)*num_words;
      memset((uint8_t *) this + topic_vec_offset, 0, num_topics*sizeof(int32_t));
      return 0;
    }

    const int32_t *get_task_list(int32_t & size){
      size = num_uwords;
      return (int32_t *) ((uint8_t *) this + word_array_offset);
    }

    const int32_t *get_wcnt_list(int32_t & size){
      size = num_uwords;
      return (int32_t *) ((uint8_t *) this + word_cnt_offset);
    }
  };
}

using namespace diskstream;
using namespace diskstream_unit;

int main(int argc, char *argv[]){

  const int KNUM_TOPICS = 100;
  const int KMAX_LINE_LENGTH = 1024*1024*2; // 2MB
  const char *diskio_endp = "inproc://diskio_endp";

  boost_po::options_description options("Allowed options");
  std::string fname;
  int32_t num_raw_buff;
  int32_t num_data_buff;
  int32_t buf_size; // number of MBs
  std::string basename;

  options.add_options()
      ("basename", boost_po::value<std::string>(&basename)->required(), "diskio base name")
      ("fname", boost_po::value<std::string>(&fname)->required(), "data file name")
      ("nrbuf", boost_po::value<int32_t>(&num_raw_buff)->default_value(1), "number of raw data buffers")
      ("ndbuf", boost_po::value<int32_t>(&num_data_buff)->default_value(1), "number of data record buffers")
      ("bsize", boost_po::value<int32_t>(&buf_size)->default_value(64), "number of MBs for each buffer");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  Buffer **raw_buffs = new Buffer *[num_raw_buff];
  Buffer **data_buffs = new Buffer *[num_data_buff];
  uint8_t **raw_buff_mem = new uint8_t *[num_raw_buff];
  uint8_t **data_buff_mem = new uint8_t *[num_data_buff];

  zmq::context_t zmq_ctx(1);
  std::cout << "creating diskio thread" << std::endl;
  DiskIOThread diskio(&zmq_ctx, 2, diskio_endp, std::string("temp/"));
  int suc = diskio.init();
  assert(suc == 0);
  suc = diskio.start();
  assert(suc == 0);
  std::cout << "diskio thread started" << std::endl;

  int32_t rawmgr = diskio.register_client();
  int32_t datamgr = diskio.register_client();

  std::cout << "successfully registered clients, rawmgr = " << rawmgr
            << " datamgr = " << datamgr << std::endl;

  struct stat fstat;
  int fstat_suc = stat(fname.c_str(), &fstat);
  int32_t fsize = fstat.st_size;
  assert(fstat_suc == 0);
  std::cout << "file size = " << fsize << std::endl;
  int32_t readbytes = 0;
  bool read_all = false;

  // read data to raw buffers, and initialize data buffers
  int32_t i;
  int32_t dbsize;
  int32_t offset = 0;
  for(i = 0; i < num_raw_buff; ++i){
    raw_buff_mem[i] = new uint8_t[buf_size*KOneMega];
    raw_buffs[i] = new (raw_buff_mem[i]) Buffer(buf_size*KOneMega, i);
    dbsize = raw_buffs[i]->get_db_size() - 512;
    diskio.sche_read_extern(rawmgr, raw_buffs[i], fname.c_str(), offset, dbsize);
    std::cout << "scheduled buffer = " << (void *) raw_buffs[i] << " to read offset = "
              << offset << " size = " << dbsize << std::endl;
    offset += dbsize;
    readbytes += dbsize;
    if(readbytes >= fsize){
      read_all = true;
      break;
    }
  }

  int32_t used_raw_buffers = (i == num_raw_buff) ? num_raw_buff : (i + 1);
  std::cout << "number of raw buffers used = " << used_raw_buffers << std::endl;
  std::cout << "all raw data reads are scheduled!" << std::endl;

  RawTextParser parser(KMAX_LINE_LENGTH, "\n");
  char *linebuf = new char[KMAX_LINE_LENGTH];

  int32_t dbuff_idx = 0;
  int32_t buff_avai; // the space in the buffer that's available
  int32_t curr_offset = 0;
  uint8_t *dst = NULL;

  data_buff_mem[dbuff_idx] = new uint8_t[buf_size*KOneMega];
  data_buffs[dbuff_idx] = new (data_buff_mem[dbuff_idx]) Buffer(buf_size*KOneMega, dbuff_idx);
  data_buffs[dbuff_idx]->set_block_id(dbuff_idx);
  dst = data_buffs[dbuff_idx]->get_dataptr(buff_avai);
  std::cout << "data_buffs idx = " << dbuff_idx << " dbptr = " << (void *) dst << std::endl;
  Document *doc, *old_doc;
  int num_docs = 0;
  int32_t databuff_to_write = 0;
  int32_t databuff_written = 0;
  i = 0;

  bool used_all_dbuff = false;
  bool canceled = false;
  while(i < used_raw_buffers && !used_all_dbuff){

    /*if(i >= 2){
      suc = diskio.cancel(rawmgr);
      assert(suc == 0);
      canceled = true;
      std::cout << "reading external is canceled!" << std::endl;
      break;
    }*/

    Buffer *rbuf;
    EMsgType type;
    int32_t bytes;
    suc = diskio.get_response(rawmgr, rbuf, type, bytes);
    assert(suc == 0);
    uint8_t *rdbptr = rbuf->get_db_ptr();
    ((char *) rdbptr)[bytes] = '\0';
    if(type == DiskIOReadExternalDone){
      std::cout << "Disk read external data done,  rbuf = " << (void *) rbuf << " read size = "
                << bytes << " bytes" << std::endl;
      parser.set_textbuf((char *) rbuf->get_db_ptr(), bytes);
      int32_t linesize = 0;

      linesize = parser.get_next(linebuf);
      while(linesize > 0){
        //std::cout << linebuf << std::endl;
        doc = new Document();
        suc = doc->pre_init(std::string(linebuf), KNUM_TOPICS);
        if(suc == -1){
          std::cout << "skip one line" << std::endl;
          delete doc;
          continue;
        }
        int32_t docsize = doc->size();
        if(buff_avai < ((int32_t) docsize + sizeof(int32_t))){
          if(dbuff_idx >= (num_data_buff - 1)){
            used_all_dbuff = true;
            delete doc;
            std::cout << "have used all data buffers!" << std::endl;
            break;
          }
          diskio.sche_write(datamgr, data_buffs[dbuff_idx], basename.c_str());
          std::cout << "write out data buffer, dbuff_idx = " << dbuff_idx << std::endl;
          ++databuff_to_write;
          ++dbuff_idx;
          data_buff_mem[dbuff_idx] = new uint8_t[buf_size*KOneMega];
          data_buffs[dbuff_idx] = new (data_buff_mem[dbuff_idx]) Buffer(buf_size*KOneMega, dbuff_idx);
          data_buffs[dbuff_idx]->set_block_id(dbuff_idx);
          dst = data_buffs[dbuff_idx]->get_dataptr(buff_avai);
          curr_offset = 0;
        }
        if(used_all_dbuff){
          break;
        }
        buff_avai -= (docsize + sizeof(int32_t));

        *((int32_t *) (dst + curr_offset)) = docsize;
        curr_offset += sizeof(int32_t);

        memcpy(dst + curr_offset, doc, sizeof(Document));
        old_doc = doc;
        doc = (Document *) (dst + curr_offset);
        curr_offset += docsize;
        doc->init();
        delete old_doc;
        ++num_docs;
        data_buffs[dbuff_idx]->append_data_record((uint8_t *) doc, doc->size() + sizeof(int32_t));
        linesize = parser.get_next(linebuf);
      }
      ++i;
    }else{
      std::cout << "Unexpected response type!" << std::endl;
    }
  }
  if(!used_all_dbuff && !canceled) {
    int32_t linesize = parser.get_last(linebuf);
    if(read_all && linesize > 0){
      std::cout << "processed the last line " << std::endl;
      std::cout << linebuf << std::endl;
      doc = new Document();
      suc = doc->pre_init(std::string(linebuf), KNUM_TOPICS);
      if(suc == -1){
        std::cout << "skip one line" << std::endl;
        delete doc;
        assert(1);
      }
      int32_t docsize = doc->size();
      if(buff_avai < ((int32_t) docsize + sizeof(int32_t))){
        if(dbuff_idx < (num_data_buff - 1)){
          diskio.sche_write(datamgr, data_buffs[dbuff_idx], basename.c_str());
          std::cout << "write out data buffer, dbuff_idx = " << dbuff_idx << std::endl;
          ++databuff_to_write;
          ++dbuff_idx;
          data_buff_mem[dbuff_idx] = new uint8_t[buf_size*KOneMega];
          data_buffs[dbuff_idx] = new (data_buff_mem[dbuff_idx]) Buffer(buf_size*KOneMega, dbuff_idx);
          data_buffs[dbuff_idx]->set_block_id(dbuff_idx);
          dst = data_buffs[dbuff_idx]->get_dataptr(buff_avai);
          curr_offset = 0;
        }else{
          delete doc;
          std::cout << "have used all data buffers!" << std::endl;
        }
      }
      buff_avai -= (docsize + sizeof(int32_t));

      *((int32_t *) (dst + curr_offset)) = docsize;
      curr_offset += sizeof(int32_t);

      memcpy(dst + curr_offset, doc, sizeof(Document));
      old_doc = doc;
      doc = (Document *) (dst + curr_offset);
      curr_offset += docsize;
      doc->init();
      delete old_doc;
      ++num_docs;
      data_buffs[dbuff_idx]->append_data_record((uint8_t *) doc, doc->size() + sizeof(int32_t));
    }
  }
  diskio.sche_write(datamgr, data_buffs[dbuff_idx], basename.c_str());
  std::cout << "write out data buffer, dbuff_idx = " << dbuff_idx << std::endl;
  ++databuff_to_write;
  std::cout << "total memory used for data buff idx = " << dbuff_idx
            << " = " << curr_offset/1024.0 << " Kbytes" << std::endl;
  std::cout << "num_of_docs = " << num_docs << std::endl;
  std::cout << "number of data buffers used = " << databuff_to_write << std::endl;

  while(databuff_written < databuff_to_write){
    Buffer *dbuf;
    EMsgType type;
    int32_t bytes;
    suc = diskio.get_response(datamgr, dbuf, type, bytes);
    if(type == DiskIOWriteDone){
      std::cout << "Disk write data done,  rbuf = " << (void *) dbuf << std::endl;
      ++databuff_written;
    }else{
      std::cout << "Unexpected response type!" << std::endl;
    }
  }

  std::cout << "I have written all data buffers to disk. To check the correctness, I write all data buffers"
               " to text file before reading from disk. Enter any char to start." << std::endl;

  char c;
  std::cin >> c;

  std::ofstream ofsb((fname + ".output.before").c_str(), std::ofstream::out | std::ofstream::trunc);

  int32_t docs_got = 0;
  for(i = 0; i < databuff_to_write; ++i){
    uint32_t data_boundary = data_buffs[i]->get_data_size();
    uint8_t *dataptr = data_buffs[i]->get_dataptr();

    std::cout << "buffer i = " << i << " dataptr = " << (void *) dataptr << std::endl;
    uint32_t coffset = 0;
    while(coffset < data_boundary){
      int32_t size = *((int32_t *) (dataptr + coffset));
      coffset += sizeof(int32_t);
      doc = (Document *) (dataptr + coffset);
      coffset += size;
      int32_t num_uwords;
      const int32_t *uwords;
      const int32_t *wcnts;
      uwords = doc->get_task_list(num_uwords);
      wcnts = doc->get_wcnt_list(num_uwords);
      //ofsb << num_uwords;
      int32_t i;
      for(i = 0; i < num_uwords; ++i){
        //ofsb << " " << uwords[i] << ":" << wcnts[i];
      }
      //ofsb << std::endl;
      ++docs_got;
      if(docs_got%5000 == 0){
        std::cout << "docs_got = " << docs_got << std::endl;
      }
    }
    std::cout << "written = " << docs_got << std::endl;
  }
  assert(docs_got == num_docs);
  suc = diskio.stop();
  assert(suc == 0);
  return 0;
}


