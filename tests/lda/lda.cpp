#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <string>
#include <sstream>
#include <assert.h>
#include <cstring>
#include <stdint.h>
#include <fstream>
#include <iostream>


namespace diskstream{

  const int32_t KOneMega = (1024*1024);
  class document_t {
  private:
    int32_t num_words;
    int32_t num_topics;
    int32_t num_uwords; //number of unique words
    int32_t word_array_offset;
    int32_t word_cnt_offset;
    int32_t topic_assign_offset;
    int32_t topic_vec_offset;

    int32_t *temp_word_list;
    int32_t *temp_wcnt_list;
  public:
    int32_t size(){
      return sizeof(document_t) + 2*num_uwords*sizeof(int32_t) + num_words*sizeof(int32_t) + num_topics*sizeof(int32_t);
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
      word_array_offset = sizeof(document_t);
      memcpy((uint8_t *) this + word_array_offset, temp_word_list, num_uwords*sizeof(int32_t));
      delete[] temp_word_list;
      temp_word_list = NULL;

      word_cnt_offset = word_array_offset + sizeof(int32_t)*num_uwords;
      memcpy((uint8_t *) this + word_cnt_offset, temp_wcnt_list, num_uwords*sizeof(int32_t));
      delete[] temp_wcnt_list;
      temp_wcnt_list = NULL;

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

int main(int argc, char *argv[]){

    const int KLINEBUF_SIZE = 1024;
    const int KNUM_TOPICS = 100;
    uint8_t *buffer = new uint8_t[64*KOneMega];
    char *linebuf = new char[KLINEBUF_SIZE];
    boost_po::options_description options("Allowed options");

    // buffer manamgement
    int32_t curr_offset = 0;
    int32_t buff_avai = 64*KOneMega;
    int32_t buff_boundary = 0;

    std::string fname;

    options.add_options()
    ("fname", boost_po::value<std::string>(&fname)->required(), "data file name");

    boost_po::variables_map options_map;
    boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
    boost_po::notify(options_map);

    std::ifstream ifs(fname.c_str(), std::ios::in);

    document_t *doc;
    int num_docs = 0;

    do{
      ifs.getline(linebuf, KLINEBUF_SIZE, '\n');
      //std::cout << "num_docs = " << num_docs << "\t"<< linebuf << std::endl;

      if(strlen(linebuf) == 0) continue;

      doc = new document_t();
      int suc = doc->pre_init(std::string(linebuf), KNUM_TOPICS);
      if(suc < 0){
        delete doc;
        continue;
      }

      int32_t size = doc->size();
      if(buff_avai < ((int32_t) size + sizeof(int32_t))) break;
      buff_avai -= (size + sizeof(int32_t));

      *((int32_t *) (buffer + curr_offset)) = size;
      curr_offset += sizeof(int32_t);

      memcpy(buffer + curr_offset, doc, sizeof(document_t));
      delete doc;

      doc = (document_t *) (buffer + curr_offset);
      curr_offset += size;
      doc->init();
      ++num_docs;
    }while(ifs.good());
    ifs.close();

    std::cout << "Total memory used = " << (64*KOneMega - buff_avai)/1024.0 << " KB" << std::endl;

    std::ofstream ofs((fname + ".output").c_str(), std::ofstream::out);

    buff_boundary = curr_offset;
    curr_offset = 0;
    int32_t docs_read = 0;

    while(curr_offset < buff_boundary){
      int32_t size = *((int32_t *) (buffer + curr_offset));
      curr_offset += sizeof(int32_t);
      doc = (document_t *) (buffer + curr_offset);
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
    assert(docs_read == num_docs);
}
