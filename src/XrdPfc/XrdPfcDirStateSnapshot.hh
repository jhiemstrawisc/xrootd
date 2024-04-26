#ifndef __XRDPFC_DIRSTATESNAPSHOT_HH__
#define __XRDPFC_DIRSTATESNAPSHOT_HH__

#include "XrdPfcDirState.hh"

#include <vector>

//==============================================================================
// Structs for DirState export in vector form
//==============================================================================

namespace XrdPfc
{

struct DirStateElement : public DirStateBase
{
   int m_parent = -1;
   int m_daughters_begin = -1, m_daughters_end = -1;

   DirStateElement() {}
   DirStateElement(const DirStateBase &b, int parent) :
     DirStateBase(b),
     m_parent(parent)
   {}
};

struct DataFsSnapshot : public DataFsStateBase
{
   std::vector<DirStateElement> m_dir_states;

   DataFsSnapshot() {}
   DataFsSnapshot(const DataFsStateBase &b) : DataFsStateBase(b) {}

   // Import of data into vector form is implemented in ResourceMonitor
   // in order to avoid dependence of this struct on DirState.

   void write_json_file(const std::string &fname, bool include_preamble);
   void dump();
};

}

#endif
