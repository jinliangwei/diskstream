
#include <sstream>
#include <string>
#include <stdint.h>
#include <string.h>

#include <diskstream_types.hpp>

#include "lda_datastruct.hpp"

namespace diskstream {

  Document::Document(int32_t _mem_size, std::string _text, int32_t _num_topics):
        num_topics(_num_topics),
        num_words(0),
        num_uwords(0){
    std::stringstream ss;
    ss << _text;
    ss >> num_uwords;
    if(num_uwords == 0) throw diskstream::DataCreateIgnore;

    // pre-check to make sure there's enough memory to hold the word list
    if(_mem_size <= (int32_t) (sizeof(Document) + sizeof(int32_t)*(num_uwords*2 + num_topics)))
      throw diskstream::DataCreateInsufficientMem;

    word_array_offset = sizeof(Document);
    word_cnt_offset = word_array_offset + sizeof(int32_t)*num_uwords;
    int32_t *word_array_ptr = (int32_t *) ((uint8_t *) this + word_array_offset);
    int32_t *word_cnt_ptr = (int32_t *) ((uint8_t *) this + word_cnt_offset);
    int32_t i = 0;
    while(ss.good()){
      char c;
      ss >> word_array_ptr[i] >> c >> word_cnt_ptr[i];
      ++i;
      ++num_words;
    }
    if(_mem_size < (int32_t) (sizeof(Document) + sizeof(int32_t)*(num_uwords*2 + num_topics + num_words)))
      throw diskstream::DataCreateInsufficientMem;
    topic_assign_offset = word_cnt_offset + sizeof(int32_t)*num_uwords;
    topic_vec_offset = topic_assign_offset + sizeof(int32_t)*num_words;
    // TODO: initialize topic_assignment and topic vector properly
  }
  int32_t Document::size(){
    return sizeof(Document) + sizeof(int32_t)*(num_uwords*2 + num_topics + num_words);
  }

  const int32_t *Document::get_task_list(int32_t & size) const{
    size = num_uwords;
    return (int32_t *) ((uint8_t *) this + word_array_offset);
  }

  const int32_t *Document::get_wcnt_list(int32_t & size) const{
    size = num_uwords;
    return (int32_t *) ((uint8_t *) this + word_cnt_offset);
  }

  Word::Word(int32_t _mem_size, int32_t _wid, int32_t _num_topics):
    wid(_wid),
    num_topics(_num_topics){
    if(_mem_size < (int32_t) (sizeof(Word) + sizeof(int32_t)*num_topics))
      throw diskstream::DataCreateInsufficientMem;
    topic_vec_offset = sizeof(Word);
    // TODO: init the topic vector properly
    memset((uint8_t *) this + topic_vec_offset, 0, num_topics*sizeof(int32_t));
  }

  int32_t Word::size() const{
    return sizeof(Word) + sizeof(int32_t)*num_topics;
  }

  int32_t Word::get_task_id() const {
    return wid;
  }

  int32_t Word::usize(int32_t _num_topics){
    return sizeof(Word) + sizeof(int32_t)*_num_topics;
  }
}
