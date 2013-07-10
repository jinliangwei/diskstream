#include <sys/time.h>
#include <stdint.h>
#include <stddef.h>
#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include <syscall.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sstream>
#include <stdlib.h>

namespace boost_po = boost::program_options;

namespace diskstream{
  const int32_t KOneMega = (1024*1024);
  const int32_t KPageSize = 1024;

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

  uint8_t *create_buffer(int32_t size, uint8_t value, int32_t &tcreate){
    uint8_t *buff = new uint8_t[size];

    int32_t i, t;
    t = timer_st();
    for(i = 0; i < size; ++i){
      buff[i] = value;
    }
    t = timer_ed(t);
    tcreate = t;
    return buff;
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

  int writen(int fd, const uint8_t *val, int32_t size){
    int32_t i = 0;
    while(i < size){
      int32_t written = write(fd, val + i, (size - i));
      if(written < 0) return written;
      i += (int32_t) written;
    }
    return 0;
  }

  int reg_write(const uint8_t *buff, int32_t size, const char *filename, int32_t &twrite, int32_t &tclose){
    int fd = open(filename, O_CREAT | O_WRONLY | O_APPEND, S_IRWXU);
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

  int dir_write(const uint8_t *buff, int32_t size, const char *filename, int32_t &twrite, int32_t &tclose){
    int fd = open(filename, O_CREAT | O_WRONLY | O_APPEND | O_DIRECT, S_IRWXU);
    int32_t t, tc;
    t = timer_st();

    int64_t align = 0xFFFFFFFFFFFFFFE0;
    /* only works on 64-bit OS */
    const uint8_t *aligned_buff = (uint8_t *) (((int64_t) (buff + 512)) & align);

    int suc = writen(fd, aligned_buff, size - 512);

    t = timer_ed(t);
    tc = timer_st();
    close(fd);
    tc = timer_ed(tc);

    twrite = t;
    tclose = tc;
    return suc;
  }

  void touch_all_pages(uint8_t **buff, int32_t nbuf, int32_t buffsize){
    int i, j, t;
    t = timer_st();
    for(i = 0; i < nbuf; ++i){
      for(j = 0; j < (buffsize/KPageSize); ++j){
        uint8_t c = buff[i][j*KPageSize];
        if(i%2){
          c = c & i;
        }else{
          c = c | i;
        }
        buff[i][j*KPageSize] = c;
      }
    }
    t = timer_ed(t);
    std::cout << "Touch all pages takes " << t << " microseconds" << std::endl;
  }

  void touch_all_pages_buff(uint8_t *buff, int32_t buffsize, uint8_t _c){
    int i, t;
    t = timer_st();
    for(i = 0; i < (buffsize/KPageSize); ++i){
      uint8_t c = buff[i*KPageSize];
      buff[i*KPageSize] = (_c & c);
    }
    t = timer_ed(t);
    //std::cout << "Touch all pages takes " << t << " microseconds" << std::endl;
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
    default:
      std::cout << "error: others" << std::endl;
      break;
    }
  }

  enum test_t{KCanonical, KDirect};

  typedef int (*wtestfunc_t)(const uint8_t *, int32_t, const char *, int32_t &, int32_t &);

  typedef int (*rtestfunc_t)(uint8_t *, int32_t, const char *, int32_t &, int32_t &, uint64_t);

  int test_write(test_t type, uint8_t **buff, int32_t nbuf,
                 int32_t &curr_buff, int32_t num_iter,
                 int32_t buff_size, const char* fname){
    int32_t twrite;
    int32_t tclose;
    int32_t ttotal = 0;
    int i;
    wtestfunc_t tfunc;
    switch(type){
    case KCanonical:
      tfunc = reg_write;
      break;
    case KDirect:
      tfunc = dir_write;
      break;
    default:
      std::cout << "error!" << std::endl;
      return -1;
      break;
    }

    for(i = 0; i < num_iter; ++i){
      int suc;
      suc = tfunc(buff[curr_buff], buff_size, fname, twrite, tclose);
      touch_all_pages_buff(buff[curr_buff], buff_size, (uint8_t) rand());
      curr_buff = (curr_buff + 1)%nbuf;
      if(suc < 0){
        display_errno();
        return -1;
      }
      std::cout << i << " " << twrite << " microseconds" << std::endl;
      ttotal += twrite;
    }
    std::cout << "average = " << ((double) ttotal)/num_iter << " microseconds" << std::endl;
    return 0;
  }

  int32_t dir_read(uint8_t *buff, int32_t size, const char *filename, int32_t &twrite,
      int32_t &topen, uint64_t offset){
    int32_t t, to;
    to = timer_st();
    int fd = open(filename, O_RDONLY | O_DIRECT | O_LARGEFILE);
    to = timer_ed(to);

    lseek(fd, offset, SEEK_SET);

    int64_t align = 0xFFFFFFFFFFFFFFE0;
    /* only works on 64-bit OS */
    uint8_t *aligned_buff = (uint8_t *) (((int64_t) (buff + 512)) & align);

    t = timer_st();
    int suc = readn(fd, aligned_buff, size - 512);
    t = timer_ed(t);

    close(fd);
    twrite = t;
    topen = to;
    return suc;
  }

  int32_t reg_read(uint8_t *buff, int32_t size, const char *filename, int32_t &twrite,
      int32_t &topen, uint64_t offset){
    int32_t t, to;
    to = timer_st();
    int fd = open(filename, O_RDONLY | O_LARGEFILE);
    to = timer_ed(to);
    lseek(fd, offset, SEEK_SET);
    t = timer_st();
    int suc = readn(fd, buff, size);
    t = timer_ed(t);

    close(fd);
    twrite = t;
    topen = to;

    return suc;
  }

  int test_read(test_t type, uint8_t **buff, int32_t nbuf,
                int32_t &curr_buff, int32_t num_iter,
                int32_t buff_size, const std::string fnamebase){
      int32_t tread;
      int32_t topen;
      int32_t ttotal = 0;
      int i;
      rtestfunc_t tfunc;
      switch(type){
      case KCanonical:
        tfunc = reg_read;
        break;
      case KDirect:
        tfunc = dir_read;
        break;
      default:
        std::cout << "error!" << std::endl;
        return -1;
        break;
      }

      for(i = 0; i < num_iter; ++i){
        int suc;
        uint64_t offset = buff_size;
        offset = offset*i;
        suc = tfunc(buff[curr_buff], buff_size, fnamebase.c_str(), tread, topen, offset);
        touch_all_pages_buff(buff[curr_buff], buff_size, (uint8_t) rand());
        curr_buff = (curr_buff + 1)%nbuf;
        if(suc < 0){
          display_errno();
          return -1;
        }
        std::cout << i << " " << tread << " microseconds" << std::endl;
        ttotal += tread;
      }
      std::cout << "average = " << ((double) ttotal)/num_iter << " microseconds" << std::endl;
      return 0;
    }
}
using namespace diskstream;

int main(int argc, char *argv[]){
  std::string wfname;
  std::string rfname;
  int32_t bsize;
  int32_t nbuf;
  //int32_t rnbuf;
  std::string ops;
  boost_po::options_description options("Allowed options");

  options.add_options()
        ("nbuf", boost_po::value<int32_t>(&nbuf)->default_value(10), "number of buffers")
        ("bsize", boost_po::value<int32_t>(&bsize)->default_value(100), "data buffer size")
        ("wfname", boost_po::value<std::string>(&wfname)->default_value("toutput.dat"),
         "write data file name")
        ("rfname", boost_po::value<std::string>(&rfname)->default_value("tinput.dat"),
         "read data file name")
        ("ops", boost_po::value<std::string>(&ops)->required(), "operations");

  /*
   * Operations:
   * w - canonical write
   * d - direct write
   * r - canonical read
   * i - direct read
   * s - stop
   * */

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  srand(5);
  int32_t buff_size = bsize*KOneMega;
  uint8_t buff_value = (uint8_t) '2';

  uint8_t **buff = new uint8_t *[nbuf];
  int32_t *tcreate = new int32_t[nbuf];
  int32_t tcreate_total = 0;
  std::cout << "create buffers" << std::endl;
  int i;
  int c;

  // create all buffers, allocate memory ahead
  for(i = 0; i < nbuf; ++i){
      buff[i] = create_buffer(buff_size, buff_value, tcreate[i]);
      std::cout << i << " " << tcreate[i] << " microsecond" << std::endl;
      tcreate_total += tcreate[i];
  }

  std::cout << "average = " << ((double) tcreate_total)/nbuf << " microseconds" << std::endl;

  touch_all_pages(buff, nbuf, buff_size);

  std::cout << "~~~~~~~~~~~~~~~~~~~~" << std::endl;

  std::stringstream ss(ops);
  bool run = true;

  std::string dir_wfname = wfname + ".dir";
  std::string reg_wfname = wfname + ".reg";

  int32_t cur_buff = 0;

  while(run){
    char c;
    std::cin >> c; //wait user instruction to continue

    char op;
    ss >> op;
    int32_t cnt;
    ss >> cnt;
    touch_all_pages(buff, nbuf, buff_size);
    std::cout << "~~~~~~~~~~~~~~~~~~~~" << std::endl;

    switch(op){
    case 'w':
      std::cout << "Canonical write, num_iter = " << cnt << std::endl;
      test_write(KCanonical, buff, nbuf, cur_buff, cnt, buff_size, reg_wfname.c_str());
      break;
    case 'd':
      std::cout << "O_DIRECT write, num_iter = " << cnt << std::endl;
      test_write(KDirect, buff, nbuf, cur_buff, cnt, buff_size, dir_wfname.c_str());
      break;
    case 'r':
      std::cout << "Canonical read, num_iter = " << cnt << std::endl;
      test_read(KCanonical, buff, nbuf, cur_buff, cnt, buff_size, rfname);
      break;
    case 'i':
      std::cout << "O_DIRECT read, num_iter = " << cnt << std::endl;
      test_read(KDirect, buff, nbuf, cur_buff, cnt, buff_size, rfname);
      break;
    case 's':
    default:
      run = false;
      break;
    }
  }

  std::cin >> c;
  for(i = 0; i < nbuf; ++i){
    delete[] buff[i];
  }
  delete buff;
  std::cin >> c;
  return 0;
}
