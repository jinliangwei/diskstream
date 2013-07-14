
#include "diskstream_util.hpp"
#include <errno.h>
#include <iostream>
#include <assert.h>
#include <string.h>

namespace diskstream {
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
  void display_errno(){
    switch(errno){
    case EAGAIN:
      std::cout << "error: EAGAIN" << std::endl;
      break;
    case EBADF:
      std::cout << "error: EBADF" << std::endl;
      break;
    case EFBIG:
      std::cout << "error: EFBIG" << std::endl;
      break;
    case EINTR:
      std::cout << "error: EINTR" << std::endl;
      break;
    case EINVAL:
      std::cout << "error: EINVAL" << std::endl;
      break;
    case EIO:
      std::cout << "error: EIO" << std::endl;
      break;
    case EFAULT:
      std::cout << "error: EFAULT" << std::endl;
      break;
    default:
      std::cout << "error: others" << std::endl;
      break;
    }
  }

  bool RawTextParser::check_last_tok_valid(char *_textbuf, int32_t bsize){
    char pot_delim = _textbuf[bsize - 1];
    bool delim_match = false;
    int i;
    for(i = 0; i < (int) delim.size(); ++i){
      if(pot_delim == delim.c_str()[i]){
        delim_match = true;
        break;
      }
    }
    if(delim_match) return true;
    return false;
  }

  RawTextParser::RawTextParser(int32_t _maxtok_size, std::string _delim):
    textbuf(NULL),
    maxtok_size(_maxtok_size + 1),
    delim(_delim),
    next_tok(NULL),
    textbuf_size(0),
    tempbuf(new char[maxtok_size + 1]), // 1 more char to accommodate '\0'
    readtempbuf(false),
    copied_size(0),
    last_tok_valid(false){}

  RawTextParser::~RawTextParser(){
    delete[] tempbuf;
  }

  // the text in buffer can be very large, call strlen may be costly
  // bszie is the number of valid chars in _textbuf, excluding '\0'
  int RawTextParser::set_textbuf(char *_textbuf, int32_t _bsize){
    if(textbuf != NULL){

      assert(next_tok != NULL);
      int32_t leftsize = strlen(next_tok);

      assert(leftsize < maxtok_size); //TODO: if larger or equal, allocate a larger buffer.
      memcpy(tempbuf, next_tok, leftsize);
      copied_size = (maxtok_size - leftsize > _bsize) ? _bsize : (maxtok_size - leftsize);
      char *copy_dst = tempbuf + leftsize;
      memcpy(copy_dst, _textbuf, copied_size);
      int32_t tempbuf_size = leftsize + copied_size;
      tempbuf[tempbuf_size] = '\0';
      readtempbuf = true;
      last_tok_valid = check_last_tok_valid(tempbuf, maxtok_size);
      next_tok = strtok(tempbuf, delim.c_str());

      textbuf = _textbuf;
      textbuf_size = _bsize;

    }else{
      textbuf = _textbuf;
      textbuf_size = _bsize;
      last_tok_valid = check_last_tok_valid(tempbuf, maxtok_size);
      next_tok = strtok(textbuf, delim.c_str());
    }
    return 0;
  }

  int32_t RawTextParser::get_next(char *token){
    if(next_tok == NULL) return 0;
    char *localnext = strtok(NULL, delim.c_str());
    int32_t tok_size;
    if(localnext != NULL){
      strcpy(token, next_tok);
      next_tok = localnext;
      tok_size = strlen(token);
    }else{
      //std::cout << "reached the end of the buffer!" << std::endl;
      // changing buffer, so last_tok_valid, strtok must be re-evaluated
      if(readtempbuf){
        //std::cout << "reading from tempbuf end" << std::endl;
        readtempbuf = false;
        if(last_tok_valid){
          //std::cout << "tempbuf last_tok_valid left in textbuf = " << (textbuf_size - copied_size) << std::endl;
          strcpy(token, next_tok);
          tok_size = strlen(token);
          textbuf += copied_size;
          last_tok_valid = check_last_tok_valid(textbuf, textbuf_size - copied_size);
          next_tok = strtok(textbuf, delim.c_str());
        }else{
          if(copied_size < textbuf_size){
            int32_t leftsize = strlen(next_tok);
            assert(copied_size > leftsize);
            textbuf += (copied_size - leftsize);
            last_tok_valid = check_last_tok_valid(textbuf, textbuf_size - (copied_size - leftsize));
            next_tok = strtok(textbuf, delim.c_str());
            tok_size = get_next(token); // recursively call get_next() as there might be a valid token
            // in textbuf
          }else{
            std::cout << "all data has been copied to tempbuf" << std::endl;
            tok_size = 0;
          }
        }
      }else{
        //std::cout << "read from textbuf end" << std::endl;
        if(last_tok_valid){
          strcpy(token, next_tok);
          next_tok = localnext;
          tok_size = strlen(token);
          textbuf = NULL;
        }else{
          tok_size = 0;
        }
      }
    }
    return tok_size;
  }

  int32_t RawTextParser::get_last(char *token){
    if(next_tok != NULL){
//    std::cout << "get_last() get whatever is left" << std::endl;
      strcpy(token, next_tok);
      next_tok = NULL;
      return strlen(token);
    }else{
      //std::cout << "get_last() nothing to return" << std::endl;
      return 0;
    }
  }

  // return index for _val or index of the largest value that's less than _val
  int32_t binary_search(const int32_t *_tlist, int32_t _len, int32_t _val){
    if(_val < _tlist[0]) return -1;
    int32_t st = 0;
    int32_t ed = _len - 1;
    int32_t m = (st + ed)/2;
    // loop invariants (when the loop condition is tested):
    // 1) st <= ed
    // 2) st~ed contains _val or the index of the largest value who is less than _val
    while(st < ed){
      if(_tlist[m] == _val) return m;
      else if(_tlist[m] > _val){
        ed = m - 1;
      }else{
        assert(m < ed);
        if(_tlist[m + 1] < _val){
          st = m + 1;
        }else if(_tlist[m + 1] == _val){
          return m + 1;
        }else{
          return m;
        }
      }
      m = (st + ed)/2;
    }
    return m;
  }

  int find_range(const int32_t *_tlist, int32_t _len, int32_t _least, int32_t _greatest,
                 int32_t &_st, int32_t &_ed){
    if(_least > _greatest) return -1;
    if(_least > _tlist[_len - 1] || _greatest < _tlist[0]) return -1;

    if(_least < _tlist[0]){
      _st = 0;
    }else{
      _st = binary_search(_tlist, _len, _least);
      if(_tlist[_st] < _least) ++_st;
    }

    if(_greatest > _tlist[_len - 1]){
      _ed = _len;
    }else{
      _ed = binary_search(_tlist, _len, _greatest) + 1;
    }
    return 0;
  }

  int32_t binary_search_exact(int32_t *array, int32_t len, int32_t val){
    int32_t begin = 0;
    int32_t end = len - 1;
    int32_t m = (begin + end)/2;
    while(array[m] != val){
      if(array[m] < val){
        begin = m + 1;
        m = (begin + end)/2;
      }else{
        end = m - 1;
        m = (begin + end)/2;
      }
      if(begin > end) return -1;
    }
    return m;
  }
}
