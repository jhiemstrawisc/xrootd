#include "XrdPfcDirStateSnapshot.hh"

#include "XrdOuc/XrdOucJson.hh"

#include <fstream>
#include <iostream>
#include <iomanip>


// Redefine to also support ordered_json ... we want to keep variable order in JSON save files.
#define PFC_DEFINE_TYPE_NON_INTRUSIVE(Type, ...)                                                \
  inline void to_json(nlohmann::json &nlohmann_json_j, const Type &nlohmann_json_t) {           \
    NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_TO, __VA_ARGS__))                    \
  }                                                                                             \
  inline void from_json(const nlohmann::json &nlohmann_json_j, Type &nlohmann_json_t) {         \
    NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_FROM, __VA_ARGS__))                  \
  }                                                                                             \
  inline void to_json(nlohmann::ordered_json &nlohmann_json_j, const Type &nlohmann_json_t) {   \
    NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_TO, __VA_ARGS__))                    \
  }                                                                                             \
  inline void from_json(const nlohmann::ordered_json &nlohmann_json_j, Type &nlohmann_json_t) { \
    NLOHMANN_JSON_EXPAND(NLOHMANN_JSON_PASTE(NLOHMANN_JSON_FROM, __VA_ARGS__))                  \
  }

namespace XrdPfc
{
PFC_DEFINE_TYPE_NON_INTRUSIVE(DirStats,
    m_NumIos, m_Duration, m_BytesHit, m_BytesMissed, m_BytesBypassed, m_BytesWritten, m_NCksumErrors,
    m_BytesRemoved, m_NFilesOpened, m_NFilesClosed, m_NFilesCreated, m_NFilesRemoved, m_NDirectoriesCreated, m_NDirectoriesRemoved)
PFC_DEFINE_TYPE_NON_INTRUSIVE(DirUsage,
    m_LastOpenTime, m_LastCloseTime, m_BytesOnDisk, m_NFilesOpen, m_NFiles, m_NDirectories)
PFC_DEFINE_TYPE_NON_INTRUSIVE(DirStateElement,
    m_dir_name, m_here_stats, m_recursive_subdir_stats, m_here_usage, m_recursive_subdir_usage,
    m_parent, m_daughters_begin, m_daughters_end)
PFC_DEFINE_TYPE_NON_INTRUSIVE(DataFsSnapshot, 
    m_usage_update_time, m_stats_reset_time,
    m_dir_states)
}

namespace
{
// Open file for writing, throw exception on failure.
void open_ofstream(std::ofstream &ofs, const std::string &fname, const char *pfx = nullptr) {
    ofs.open(fname, std::ofstream::trunc);
    if (!ofs) {
        char m[2048];
        snprintf(m, 2048, "%s%sError opening %s for write: %m", pfx ? pfx : "", pfx ? " " : "", fname.c_str());
        throw std::runtime_error(m);
    }
}
}

using namespace XrdPfc;

void DataFsSnapshot::write_json_file(const std::string &fname, bool include_preamble)
{
    std::ofstream ofs;
    open_ofstream(ofs, fname, __func__);

    if (include_preamble) {
        ofs << "{ \"dirstate_snapshot\": ";
    }

    nlohmann::ordered_json j;
    to_json(j, *this);

    ofs << std::setw(1);
    ofs << j;

    if (include_preamble) {
        ofs << " }";
    }

    ofs << "\n";
    ofs.close();
    return 0;
}

void DataFsSnapshot::dump()
{
    nlohmann::ordered_json j; //  = *this;
    to_json(j, *this);
    std::cout << j.dump(3) << "\n";
}
