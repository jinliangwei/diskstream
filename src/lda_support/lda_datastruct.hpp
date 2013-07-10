#ifndef __LDA_DATASTRUC_HPP__
#define __LDA_DATASTRUC_HPP__

namespace diskstream {
  class Document {
  private:
      int32_t num_topics;
      int32_t num_words;
      int32_t num_uwords; // number of unique words
      int32_t word_array_offset; // array of unique word
      int32_t word_cnt_offset; // number of appearance for each unique word
      int32_t topic_assign_offset; // topic assignment for each word
      int32_t topic_vec_offset; // number of words assigned to each topic

  public:
      Document(int32_t _mem_size, std::string _text, int32_t _num_topics);
      ~Document();
      int32_t size();
      const int32_t *get_task_list(int32_t &size) const;
      const int32_t *get_wcnt_list(int32_t &size) const;
  };

  class Word {
  private:
    int32_t wid; // word id
    int32_t num_topics;
    int32_t topic_vec_offset;
  public:
    Word(int32_t _mem_size, int32_t _wid, int32_t _num_topics);
    ~Word();
    int32_t size() const;
    int32_t get_task_id() const;

    static int32_t usize(int32_t _num_topics);
  };
}


#endif /* LDA_DATASTRUC_HPP_ */
