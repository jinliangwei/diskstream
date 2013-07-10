/*
 * diskstream_util.hpp
 *
 *  Created on: Jun 24, 2013
 *      Author: jinliang
 */

#ifndef __DISKSTREAM_UTIL_HPP__
#define __DISKSTREAM_UTIL_HPP__

#include <sys/time.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "diskstream_buffer.hpp"

namespace diskstream {

  typedef struct timeval timeval_t;

  int32_t get_micro_second(timeval_t tv);

  // return starting time in microseconds
  int32_t timer_st();

  // return ending time in microseconds
  int32_t timer_ed(int32_t stms);

  void display_errno();

  class RawTextParser {
    private:
      char *textbuf;
      int32_t maxtok_size; // maximum token size - this is actually maximum token size + 1 because
                           // the tempbuf needs to leave a one-char space for the delim; this is handled
                          // by the constructor
      std::string delim;
      char *next_tok;
      int32_t textbuf_size;  //number of characters in textbuf, not including '\0', but the last char
                              // in textbuf must be '\0'; this is equivalent to call strlen() on textbuf
      char *tempbuf;
      bool readtempbuf; // current reading from temp buff
      int32_t copied_size; // size of data copied for textbuf to tempbuf
      bool last_tok_valid;
      /*
       * check_last_tok_valid() is necessary because strtok() does not distinguish between the last
       * char is a NULL or is a delim. In case the last char is indeed a delim, the last char is
       * indeed a delim, it will be replaced with a NULL. Therefore, we cannot determine the last token
       * returned is a complete token or an incomplete one.
       * */

      bool check_last_tok_valid(char *_textbuf, int32_t bsize);
    public:
      RawTextParser(int32_t _maxtok_size, std::string _delim);
      ~RawTextParser();
      // the text in buffer can be very large, call strlen may be costly
      // bszie is the number of valid chars in _textbuf, excluding '\0'
      int set_textbuf(char *_textbuf, int32_t _bsize);
      int32_t get_next(char *token);
      int32_t get_last(char *token);
    };

  class MemChunkAccesser {
  private:
    int32_t curr_offset;
    int32_t max_offset; // buffer data block boundary, pointing to the last available byte
    uint8_t *dataptr;
  public:
    MemChunkAccesser():
      curr_offset(0),
      max_offset(0),
      dataptr(NULL){}

    void set_mem(uint8_t *_dataptr, int32_t _size){
      dataptr = _dataptr;
      max_offset = _size - 1;
      curr_offset = 0;
    }
    uint8_t *curr_ptr() const{
      return dataptr + curr_offset;
    }
    int32_t avai_size() const{
      if(max_offset >= curr_offset)
        return max_offset - curr_offset + 1;
      else
        return 0;
    }
    // at most advance to max_offset + 1
    // so successful advance does not mean curr_ptr is valid
    int advance(int32_t _size){
      if(curr_offset + _size > max_offset + 1) return -1;
      curr_offset += _size;
      return 0;
    }
    int goback(int32_t _size){
      if(curr_offset < _size) return -1;
      curr_offset -= _size;
      return 0;
    }
  };
}

#endif /* DISKSTREAM_UTIL_HPP_ */
