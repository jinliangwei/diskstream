#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <diskstream_buffer.hpp>
#include <lda_support/diskstream_lda_executor.hpp>

#include <stdint.h>
#include <string>
#include <vector>
#include <assert.h>
#include <iostream>

using namespace diskstream;

int main(int argc, char *argv[]){

  const uint32_t KMAX_LINE_LENGTH = 1024*1024*2; // 2MB
  const int64_t KOneMega = (1024*1024);

  std::string datapath;
  int32_t buff_size; // number of MBs
  int32_t membudget;
  std::string basename;
  int32_t num_words;
  int32_t num_topics;
  std::string datadir;

  boost_po::options_description options("Allowed options");
  options.add_options()
      ("datadir", boost_po::value<std::string>(&datadir)->required(), "diskio data directory")
      ("basename", boost_po::value<std::string>(&basename)->required(), "diskio base name")
      ("datapath", boost_po::value<std::string>(&datapath)->required(), "data file name")
      ("membudget", boost_po::value<int32_t>(&membudget)->required(), "membudget in MBs")
      ("bsize", boost_po::value<int32_t>(&buff_size)->default_value(64), "number of MBs for each buffer")
      ("nwords", boost_po::value<int32_t>(&num_words)->required(), "number of words")
      ("ntopics", boost_po::value<int32_t>(&num_topics)->default_value(100), "number of topics");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  std::cout << "diskstream lda starts here" << std::endl;

  LdaExecutorInit lda_executor;
  std::vector<std::string> extern_data(1);
  extern_data[0] = datapath;

  int suc = lda_executor.init(membudget*KOneMega, buff_size*KOneMega, num_words, num_topics, datadir,
                              extern_data, basename, KMAX_LINE_LENGTH);
  assert(suc == 0);

  suc = lda_executor.run();
  assert(suc == 0);

  lda_executor.cleanup();
  return 0;
}
