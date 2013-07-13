#include <util/diskstream_util.hpp>
#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <stdint.h>

using namespace diskstream;

int main(int argc, char *argv[]){
  int32_t le, ge;
  boost_po::options_description options("Allowed options");
    options.add_options()
        ("le", boost_po::value<int32_t>(&le)->required(), "range start")
        ("ge", boost_po::value<int32_t>(&ge)->required(), "range end");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  int32_t tasks[] = {2, 3, 5, 7, 12, 35, 88, 123, 145, 179, 220};
  int32_t len = 11;
  int32_t st, ed;

  int suc = find_range(tasks, 11, le, ge, st, ed);
  if(suc == 0){
    std::cout << "st= " << st << " ed = " << ed << std::endl;
    if(ed < len){
      std::cout << "tasks[st] = " << tasks[st] << " tasks[ed] = " << tasks[ed] << std::endl;
    }
  }else{
    std::cout << "out of range" << std::endl;
  }
  return 0;
}
