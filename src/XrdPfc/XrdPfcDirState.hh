#ifndef __XRDPFC_DIRSTATE_HH__
#define __XRDPFC_DIRSTATE_HH__

#include "XrdPfcStats.hh"

#include <ctime>
#include <map>
#include <string>


//==============================================================================
// Manifest:
//------------------------------------------------------------------------------
// - Data-holding struct DirUsage -- complementary to Stats.
// - Base classes for DirState and DataFsState, shared between in-memory
//   tree form and snap-shot vector form.
// - Structs for DirState export in vector form:
//   - struct DirStateElement, and
//   - struct DataFsSnapshot.
//   Those should probably go to another .hh/.cc so the object file can be included
//   the dedicated binary for processing of the binary dumps.
// - class DirState -- state of a directory, including current delta-stats.
// - class DataFSState -- manager of the DirState tree, starting from root (as in "/").
//
// Structs for DirState export in vector form (DirStateElement and DataFsSnapshot)
// are declared in XrdPfcDirStateSnapshot.hh.

//==============================================================================


namespace XrdPfc
{
class PathTokenizer;

//==============================================================================
// Data-holding struct DirUsage -- complementary to Stats.
//==============================================================================

struct DirUsage
{
   time_t    m_LastOpenTime  = 0;
   time_t    m_LastCloseTime = 0;
   long long m_BytesOnDisk   = 0;
   int       m_NFilesOpen    = 0;
   int       m_NFiles        = 0;
   int       m_NDirectories  = 0;

   void update_from_stats(const DirStats& s)
   {
      m_BytesOnDisk  += s.m_BytesWritten        - s.m_BytesRemoved;
      m_NFilesOpen   += s.m_NFilesOpened        - s.m_NFilesClosed;
      m_NFiles       += s.m_NFilesCreated       - s.m_NFilesRemoved;
      m_NDirectories += s.m_NDirectoriesCreated - s.m_NDirectoriesRemoved;
   }

   void update_last_times(const DirUsage& u)
   {
      m_LastOpenTime  = std::max(m_LastOpenTime,  u.m_LastOpenTime);
      m_LastCloseTime = std::max(m_LastCloseTime, u.m_LastCloseTime);
   }
};


//==============================================================================
// Base classes, shared between in-memory tree form and snap-shot vector form.
//==============================================================================

struct DirStateBase
{
   std::string  m_dir_name;

   DirStats     m_here_stats;
   DirStats     m_recursive_subdir_stats;

   DirUsage     m_here_usage;
   DirUsage     m_recursive_subdir_usage;


   DirStateBase() {}
   DirStateBase(const std::string &dname) : m_dir_name(dname) {}

   const DirUsage& recursive_subdir_usage() const { return m_recursive_subdir_usage; }
};

struct DataFsStateBase
{
   time_t    m_usage_update_time = 0;
   time_t    m_stats_reset_time = 0;

   // FS usage and available space information
};


//==============================================================================
// Structs for DirState export in vector form
//==============================================================================

struct DirStateElement;
struct DataFsSnapshot;

//==============================================================================
// DirState
//==============================================================================

struct DirState : public DirStateBase
{
   typedef std::map<std::string, DirState> DsMap_t;
   typedef DsMap_t::iterator               DsMap_i;

   DirState    *m_parent = nullptr;
   DsMap_t      m_subdirs;

   int          m_depth;
   // bool      m_stat_report;  // not used yet - storing of stats requested; might also need depth

   // XXX int m_possible_discrepancy; // num detected possible inconsistencies. here, subdirs?

   // flag - can potentially be inaccurate -- plus timestamp of it (min or max, if several for subdirs)?

   // Do we need running averages of these, too, not just traffic?
   // Snapshot should be fine, no?

   // Do we need all-time stats? Files/dirs created, deleted; files opened/closed;
   // Well, those would be like Stats-running-average-infinity, just adding stuff in.

   // min/max open (or close ... or both?) time-stamps

   // Do we need string name? Probably yes, if we want to construct PFN from a given
   // inner node upwards. Also, in this case, is it really the best idea to have
   // map<string, DirState> as daughter container? It keeps them sorted for export :)

   // quota info, enabled?


   void init();

   DirState* create_child(const std::string &dir);

   DirState* find_path_tok(PathTokenizer &pt, int pos, bool create_subdirs,
                           DirState **last_existing_dir = nullptr);


   DirState();

   DirState(DirState *parent);

   DirState(DirState *parent, const std::string& dname);

   DirState* get_parent() { return m_parent; }

   DirState* find_path(const std::string &path, int max_depth, bool parse_as_lfn, bool create_subdirs,
                       DirState **last_existing_dir = nullptr);

   DirState* find_dir(const std::string &dir, bool create_subdirs);


   void upward_propagate_stats_and_times();
   void apply_stats_to_usages();
   void reset_stats();

   // attic
   long long upward_propagate_usage_purged(); // why would this be any different? isn't it included in stats?

   int count_dirs_to_level(int max_depth) const;

   void dump_recursively(const char *name, int max_depth) const;
};


//==============================================================================
// DataFsState
//==============================================================================

struct DataFsState : public DataFsStateBase
{
   DirState        m_root;
   mutable time_t  m_prev_time;


   DataFsState() :
      m_root      (),
      m_prev_time (time(0))
   {}

   DirState* get_root() { return & m_root; }

   DirState* find_dirstate_for_lfn(const std::string& lfn, DirState **last_existing_dir = nullptr)
   {
      return m_root.find_path(lfn, -1, true, true, last_existing_dir);
   }

   void upward_propagate_stats_and_times();
   void apply_stats_to_usages();
   void reset_stats();

   // attic
   void upward_propagate_usage_purged() { m_root.upward_propagate_usage_purged(); }

   void dump_recursively(int max_depth) const;
};

}

#endif
