#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <diskstream_iothread.hpp>
#include <vector>

using namespace diskstream;
int main(int argc, char *argv[]){

  std::string basename;
  const char *diskio_endp = "inproc://diskio_endp";

  boost_po::options_description options("Allowed options");
  options.add_options()
          ("basename", boost_po::value<std::string>(&basename)->required(), "diskio base name");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  zmq::context_t zmq_ctx(1);
  std::cout << "creating diskio thread" << std::endl;
  DiskIOThread diskio(&zmq_ctx, 2, diskio_endp, std::string("temp/"));
  int suc = diskio.init();
  assert(suc == 0);

  std::vector<uint32_t> fsizes = diskio.get_all_sizes(basename);

  assert(fsizes.size() > 0);
  int i;
  for(i = 0; i < (int) fsizes.size(); ++i){
    std::cout << " i = " << i << ", size = " << fsizes[i] << std::endl;
  }
  return 0;
}
