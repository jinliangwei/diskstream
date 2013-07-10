#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <sys/time.h>
#include <string>
#include <sstream>
#include <assert.h>
#include <cstring>
#include <stdint.h>
#include <fstream>
#include <iostream>
#include <syscall.h>
#include <fcntl.h>

#include <diskstream_buffer.hpp>


namespace diskstream_unit{

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
      return sizeof(Document) + 2*num_uwords*sizeof(int32_t) + num_words*sizeof(int32_t) + num_topics*sizeof(int32_t);
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

  typedef struct timeval timeval_t;

  int32_t get_micro_second(timeval_t tv){
    return (int32_t) tv.tv_sec*1000000 + tv.tv_usec;
  }

  int32_t timer_st(){
    timeval_t t;
    gettimeofday(&t, NULL);
    return get_micro_second(t);
  }

  int32_t timer_ed(int32_t stms){
    timeval_t t;
    gettimeofday(&t, NULL);
    return get_micro_second(t) - stms;
  }

  int writen(int fd, const uint8_t *val, int32_t size){
    int32_t i = 0;
    while(i < size){
      int32_t written = write(fd, val + i, (size - i));
      if(written < 0) return written;
      i += (int32_t) written;
    }
    return 0;
  }

  int dir_write(const uint8_t *buff, int32_t size, const char *filename, int32_t &twrite, int32_t &tclose){
    int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT, S_IRWXU);
    int32_t t, tc;
    t = timer_st();

    int suc = writen(fd, buff, size);

    t = timer_ed(t);
    tc = timer_st();
    close(fd);
    tc = timer_ed(tc);

    twrite = t;
    tclose = tc;
    return suc;
   }

  int32_t readn(int fd, uint8_t *val, int32_t size){
    int32_t readtotal = 0;
    do{
      int32_t readbytes = read(fd, val, size);
      if(readbytes == 0){
        return readtotal;
      }else if(readbytes < 0){
        return readbytes;
      }else{
        readtotal += readbytes;
      }
    }while(readtotal < size);
    return readtotal;
  }

  int32_t dir_read(uint8_t *buff, int32_t size, const char *filename, int32_t &twrite, int32_t &topen){
    int32_t t, to;
    to = timer_st();
    int fd = open(filename, O_RDONLY | O_DIRECT);
    to = timer_ed(to);

    t = timer_st();
    int suc = readn(fd, buff, size);
    t = timer_ed(t);

    close(fd);
    twrite = t;
    topen = to;
    return suc;
  }
}


using namespace diskstream;
using namespace diskstream_unit;

int main(int argc, char *argv[]){

  const int KLINEBUF_SIZE = 1024*1024;
  const int KNUM_TOPICS = 100;
  const int KNUM_MEGAS = 1000;
  uint8_t *mem = new uint8_t[KNUM_MEGAS*KOneMega];
  Buffer *buffer = new (mem) Buffer(KNUM_MEGAS*KOneMega, 0);
  char *linebuf = new char[KLINEBUF_SIZE];

  boost_po::options_description options("Allowed options");

  int32_t buff_avai;
  int32_t buff_total;
  uint8_t *dst = buffer->get_dataptr(buff_avai);
  int32_t curr_offset = 0;
  int32_t buff_boundary;
  buff_total = buff_avai;

  std::cout << "data begins at: " << (void *) dst << " buffer size = "
      << buff_avai/KOneMega << " MegaBytes" << std::endl;

  std::string fname;

  options.add_options()
    ("fname", boost_po::value<std::string>(&fname)->required(), "data file name");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  std::ifstream ifs(fname.c_str(), std::ios::in);

  Document *doc, *old_doc;
  int num_docs = 0;

  do{
    ifs.getline(linebuf, KLINEBUF_SIZE, '\n');
    //std::cout << linebuf << std::endl;

    if(strlen(linebuf) == 0) continue;

    doc = new Document();
    int suc = doc->pre_init(std::string(linebuf), KNUM_TOPICS);
    if(suc == -1){
      delete doc;
      continue;
    }

    int32_t size = doc->size();
    if(buff_avai < ((int32_t) size + sizeof(int32_t))) break;
    buff_avai -= (size + sizeof(int32_t));

    *((int32_t *) (dst + curr_offset)) = size;
    curr_offset += sizeof(int32_t);

    memcpy(dst + curr_offset, doc, sizeof(Document));
    old_doc = doc;

    doc = (Document *) (dst + curr_offset);
    curr_offset += size;
    doc->init();
    delete old_doc;
    ++num_docs;
    buffer->append_data_record((uint8_t *) doc, doc->size());
  }while(ifs.good());
  ifs.close();

  std::cout << "num_docs = " << num_docs << std::endl;
  std::cout << "Total memory used = " << (buff_total - buff_avai)/1024.0 << " KB" << std::endl;


  std::cout << "write to file before manipulate buffer" << std::endl;

  std::ofstream ofsb((fname + ".output.before").c_str(), std::ofstream::out | std::ofstream::trunc);

  buff_boundary = curr_offset;
  curr_offset = 0;
  int32_t docs_read = 0;

  while(curr_offset < buff_boundary){
    int32_t size = *((int32_t *) (dst + curr_offset));
    curr_offset += sizeof(int32_t);
    doc = (Document *) (dst + curr_offset);
    curr_offset += size;
    int32_t num_uwords;
    const int32_t *uwords;
    const int32_t *wcnts;
    uwords = doc->get_task_list(num_uwords);
    wcnts = doc->get_wcnt_list(num_uwords);
    ofsb << num_uwords;
    int32_t i;
    for(i = 0; i < num_uwords; ++i){
      ofsb << " " << uwords[i] << ":" << wcnts[i];
    }
    ofsb << std::endl;
    ++docs_read;
  }
  ofsb.close();
  std::cout << "docs_read = " << docs_read << std::endl;
  assert(docs_read == num_docs);

  uint32_t num_docs_in_buffer = buffer->get_num_data();
  assert(num_docs == (int32_t) num_docs_in_buffer);

  uint32_t wsize = buffer->get_db_size();
  uint8_t *wblock = buffer->get_db_ptr();

  int32_t twrite, tclose, tread, topen;
  dir_write(wblock, wsize, (fname + ".buf").c_str(), twrite, tclose);
  std::cout << "write " << " wsize = " << wsize/1024 << " KB takes " << twrite << " microseconds" << std::endl;

  memset(wblock, 0, wsize);

  dir_read(wblock, wsize, (fname + ".buf").c_str(), tread, topen);
  std::cout << "read " << " wsize = " << wsize/1024 << " KB takes " << tread << " microseconds" << std::endl;

  std::ofstream ofs((fname + ".output").c_str(), std::ofstream::out | std::ofstream::trunc);

  curr_offset = 0;
  docs_read = 0;

  while(curr_offset < buff_boundary){
    int32_t size = *((int32_t *) (dst + curr_offset));
    curr_offset += sizeof(int32_t);
    doc = (Document *) (dst + curr_offset);
    curr_offset += size;
    int32_t num_uwords;
    const int32_t *uwords;
    const int32_t *wcnts;
    uwords = doc->get_task_list(num_uwords);
    wcnts = doc->get_wcnt_list(num_uwords);
    ofs << num_uwords;
    int32_t i;
    for(i = 0; i < num_uwords; ++i){
      ofs << " " << uwords[i] << ":" << wcnts[i];
    }
    ofs << std::endl;
    ++docs_read;
  }
  ofs.close();
  std::cout << "docs_read = " << docs_read << std::endl;
  assert(docs_read == num_docs);
}
