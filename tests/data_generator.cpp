#include <boost/program_options.hpp>
namespace boost_po = boost::program_options;

#include <string>

#include <data_generator/diskstream_lda_datagen.hpp>

using namespace diskstream;

int main(int argc, char **argv){
  int32_t num_words;
  int32_t num_docs;
  int32_t max_word_freq;
  int32_t max_uwords_per_doc;
  int32_t min_uwords_per_doc;
  std::string basename;
  std::string datadir;

  const int32_t KMAX_LINE_LENGTH = 1024*1024*2;

  boost_po::options_description options("Allowed options");
  options.add_options()
        ("datadir", boost_po::value<std::string>(&datadir)->required(), "output data directory")
        ("basename", boost_po::value<std::string>(&basename)->required(), "basename")
        ("nwords", boost_po::value<int32_t>(&num_words)->required(), "number of words")
        ("ndocs", boost_po::value<int32_t>(&num_docs)->required(), "number of documents")
        ("wfreq", boost_po::value<int32_t>(&max_word_freq)->required(), "maximum word frequency")
        ("maxwords", boost_po::value<int32_t>(&max_uwords_per_doc)->required(), "maximum uwords per doc")
        ("minwords", boost_po::value<int32_t>(&min_uwords_per_doc)->required(), "minimum uwords per doc");

  boost_po::variables_map options_map;
  boost_po::store(boost_po::parse_command_line(argc, argv, options), options_map);
  boost_po::notify(options_map);

  LdaDataGen dgen;

  int suc = dgen.init(num_words, num_docs, max_word_freq, max_uwords_per_doc, min_uwords_per_doc, basename,
             datadir, KMAX_LINE_LENGTH);
  assert(suc == 0);

  suc = dgen.run();
  assert(suc == 0);

  return 0;

}
