#include "XrdPfcResourceMonitor.hh"
#include "XrdPfc.hh"
#include "XrdPfcPathParseTools.hh"
#include "XrdPfcFsTraversal.hh"
#include "XrdPfcDirState.hh"
#include "XrdPfcDirStateSnapshot.hh"
#include "XrdPfcTrace.hh"

using namespace XrdPfc;

namespace
{
   XrdSysTrace* GetTrace() { return Cache::GetInstance().GetTrace(); }
   const char *m_traceID = "ResourceMonitor";
}

ResourceMonitor::ResourceMonitor(XrdOss& oss) :
   m_fs_state(new DataFsState),
   m_oss(oss)
{}

ResourceMonitor::~ResourceMonitor()
{
   delete m_fs_state;
}

//------------------------------------------------------------------------------
// Initial scan
//------------------------------------------------------------------------------

void ResourceMonitor::scan_dir_and_recurse(FsTraversal &fst)
{
   printf("In scan_dir_and_recurse for '%s', size of dir_vec = %d, file_stat_map = %d\n",
           fst.m_current_path.c_str(),
          (int)fst.m_current_dirs.size(), (int)fst.m_current_files.size());

   // Breadth first, accumulate into "here"
   DirUsage &here = fst.m_dir_state->m_here_usage;
   for (auto it = fst.m_current_files.begin(); it != fst.m_current_files.end(); ++it)
   {
      printf("would be doing something with %s ... has_data=%d, has_cinfo=%d\n",
              it->first.c_str(), it->second.has_data, it->second.has_cinfo);

      // XXX Make some of these optional?
      // Remove files that do not have both cinfo and data?
      // Remove empty directories before even descending?
      // Leave this for some consistency pass?
      // Note that FsTraversal supports ignored paths ... some details (config, N2N) to be clarified.

      if (it->second.has_data && it->second.has_cinfo) {
         here.m_BytesOnDisk += it->second.stat_data.st_blocks * 512;
         here.m_NFiles      += 1;
      }
   }

   // Sub-dirs second, accumulate into "subdirs".
   DirUsage &subdirs = fst.m_dir_state->m_recursive_subdir_usage;
   std::vector<std::string> dirs;
   dirs.swap(fst.m_current_dirs);
   for (auto &dname : dirs)
   {
      if (fst.cd_down(dname))
      {
         DirState *daughter = fst.m_dir_state;
         DirUsage &dhere    = daughter->m_here_usage;
         DirUsage &dsubdirs = daughter->m_recursive_subdir_usage;

         scan_dir_and_recurse(fst);
         fst.cd_up();

         here.m_NDirectories += 1;

         subdirs.m_BytesOnDisk  += dhere.m_BytesOnDisk  + dsubdirs.m_BytesOnDisk;
         subdirs.m_NFiles       += dhere.m_NFiles       + dsubdirs.m_NFiles;
         subdirs.m_NDirectories += dhere.m_NDirectories + dsubdirs.m_NDirectories;
      }
      // XXX else try to remove it?
   }
}

bool ResourceMonitor::perform_initial_scan()
{
   // Called after PFC configuration is complete, but before full startup of the daemon.
   // Base line usages are accumulated as part of the file-system, traversal.

   bool success_p = true;

   FsTraversal fst(m_oss);
   fst.m_protected_top_dirs.insert("pfc-stats"); // XXXX This should come from config. Also: N2N?

   if (fst.begin_traversal(m_fs_state->get_root(), "/"))
   {
      scan_dir_and_recurse(fst);
   }
   else
   {
      // Fail startup, can't open /.
      success_p = false;
   }
   fst.end_traversal();

   return success_p;
}

//------------------------------------------------------------------------------
// Processing of queues
//------------------------------------------------------------------------------

int ResourceMonitor::process_queues()
{
   static const char *trc_pfx = "process_queues() ";

   // Assure that we pick up only entries that are present now.
   // We really want all open records to be processed before file-stats updates
   // and all those before the close records.
   // Purges are sort of tangential as they really just modify bytes / number
   // of files in a direcotry and do not deal with any persistent file id tokens.

   int n_records = 0;
   {
      XrdSysMutexHelper _lock(&m_queue_mutex);
      n_records += m_file_open_q.swap_queues();
      n_records += m_file_update_stats_q.swap_queues();
      n_records += m_file_close_q.swap_queues();
      n_records += m_file_purge_q1.swap_queues();
      n_records += m_file_purge_q2.swap_queues();
      n_records += m_file_purge_q3.swap_queues();
      ++m_queue_swap_u1;
   }

   for (auto &i : m_file_open_q.read_queue())
   {
      // i.id: LFN, i.record: OpenRecord
      int tid = i.id;
      AccessToken &at = token(tid);
      printf("process file open for token %d, time %ld -- %s\n", tid, i.record.m_open_time, at.m_filename.c_str());

      // Resolve fname into DirState.
      // We could clear the filename after this ... or keep it, should we need it later on.
      // For now it is just used for debug printouts.
      DirState *last_existing_ds = nullptr;
      DirState *ds = m_fs_state->find_dirstate_for_lfn(at.m_filename, &last_existing_ds);
      at.m_dir_state = ds;
      ds->m_here_stats.m_NFilesOpened += 1;

      // If this is a new file figure out how many new parent dirs got created along the way.
      if ( ! i.record.m_existing_file) {
         ds->m_here_stats.m_NFilesCreated += 1;
         DirState *pp = ds;
         while (pp != last_existing_ds) {
            pp = pp->get_parent();
            pp->m_here_stats.m_NDirectoriesCreated += 1;
         }
      }

      ds->m_here_usage.m_LastOpenTime = i.record.m_open_time;
   }

   for (auto &i : m_file_update_stats_q.read_queue())
   {
      // i.id: token, i.record: Stats
      int tid = i.id;
      AccessToken &at = token(tid);
      // Stats
      DirState *ds = at.m_dir_state;
      printf("process file update for token %d, %p -- %s\n",
             tid, ds, at.m_filename.c_str());

      ds->m_here_stats.AddUp(i.record);
   }

   for (auto &i : m_file_close_q.read_queue())
   {
      // i.id: token, i.record: CloseRecord
      int tid = i.id;
      AccessToken &at = token(tid);
      printf("process file close for token %d, time %ld -- %s\n",
             tid, i.record.m_close_time, at.m_filename.c_str());

      DirState *ds = at.m_dir_state;
      ds->m_here_stats.m_NFilesClosed += 1;

      ds->m_here_usage.m_LastCloseTime = i.record.m_close_time;

      // If full-stats write is not a multiple of 512-bytes that means we wrote the
      // tail block. As usage is measured in 512-byte blocks this gets rounded up here.
      long long over_512b = i.record.m_full_stats.m_BytesWritten & 0x1FFll;
      if (over_512b)
         ds->m_here_stats.m_BytesWritten += 512ll - over_512b;

      // Release the AccessToken!
      at.clear();
      m_access_tokens_free_slots.push_back(tid);
   }

   for (auto &i : m_file_purge_q1.read_queue())
   {
      // i.id: DirState*, i.record: PurgeRecord
      DirState *ds = i.id;
      // NOTE -- bytes removed should already be rounded up to 512-bytes for each file.
      ds->m_here_stats.m_BytesRemoved  += i.record.m_total_size;
      ds->m_here_stats.m_NFilesRemoved += i.record.n_files;
   }
   for (auto &i : m_file_purge_q2.read_queue())
   {
      // i.id: directory-path, i.record: PurgeRecord
      DirState *ds = m_fs_state->get_root()->find_path(i.id, -1, false, false);
      if ( ! ds) {
         TRACE(Error, trc_pfx << "DirState not found for directory path '" << i.id << "'.");
         // find_path can return the last dir found ... but this clearly isn't a valid purge record.
         continue;
      }
      // NOTE -- bytes removed should already be rounded up to 512-bytes for each file.
      ds->m_here_stats.m_BytesRemoved  += i.record.m_total_size;
      ds->m_here_stats.m_NFilesRemoved += i.record.n_files;
   }
   for (auto &i : m_file_purge_q3.read_queue())
   {
      // i.id: LFN, i.record: size of file
      DirState *ds = m_fs_state->get_root()->find_path(i.id, -1, true, false);
      if ( ! ds) {
         TRACE(Error, trc_pfx << "DirState not found for LFN path '" << i.id << "'.");
         continue;
      }
      // NOTE -- bytes removed should already be rounded up to 512-bytes.
      ds->m_here_stats.m_BytesRemoved  += i.record;
      ds->m_here_stats.m_NFilesRemoved += 1;
   }

   // Read queues / vectors are cleared at swap time.
   // We might consider reducing their capacity by half if, say, their usage is below 25%.

   return n_records;
}

//------------------------------------------------------------------------------
// Heart beat
//------------------------------------------------------------------------------

void ResourceMonitor::heart_beat()
{
   printf("RMon entering heart_beat!\n");

   // initial scan performed as part of config

   time_t now = time(0);
   time_t next_queue_proc_time = now + 10;
   time_t next_up_prop_time    = now + 60;

   while (true)
   {
      time_t start = time(0);
      time_t next_event = std::min(next_queue_proc_time, next_up_prop_time);
      if (next_event > start)
      {
         unsigned int t_sleep = next_event - start;
         printf("sleeping for %u seconds, to be improved ...\n", t_sleep);
         sleep(t_sleep);
      }

      int n_processed = process_queues();
      next_queue_proc_time += 10;
      printf("processed %d records\n", n_processed);

      if (next_up_prop_time > time(0))
         continue;

      m_fs_state->upward_propagate_stats_and_times();
      next_up_prop_time += 60;

      // Here we can learn assumed file-based usage
      // run the "disk-usage"
      // decide if age-based purge needs to be run (or uvkeep one)
      // decide if the standard / plugin purge needs to be called


      // Check time, is it time to export into vector format, to disk, into /pfc-stats.
      // This one should really be rather timely ... as it will be used for calculation
      // of averages of stuff going on.

      m_fs_state->apply_stats_to_usages();

      // Dump statistcs before actual purging so maximum usage values get recorded.
      // This should dump out binary snapshot into /pfc-stats/, if so configured.
      // Also, optionally, json.
      // Could also go to gstream but this could easily be way too large.
      if (Cache::Conf().is_dir_stat_reporting_on())
      {
         const int       StoreDepth   =  Cache::Conf().m_dirStatsStoreDepth;
         const DirState &root_ds      = *m_fs_state->get_root();
         const int       n_sshot_dirs =  root_ds.count_dirs_to_level(StoreDepth);
         printf("Snapshot n_dirs=%d, total n_dirs=%d\n", n_sshot_dirs,
               root_ds.m_here_usage.m_NDirectories + root_ds.m_recursive_subdir_usage.m_NDirectories + 1);

         m_fs_state->dump_recursively(StoreDepth);

         DataFsSnapshot ss(*m_fs_state);
         ss.m_dir_states.reserve(n_sshot_dirs);

         ss.m_dir_states.emplace_back( DirStateElement(root_ds, -1) );
         fill_sshot_vec_children(root_ds, 0, ss.m_dir_states, StoreDepth);

         ss.dump();
      }

      // if needed export to vector form
      // - write to file
      // - pass to purge pin

      m_fs_state->reset_stats();

      // check time / diskusage --> purge condition?
      // run purge as job or thread
      // m_fs_state->upward_propagate_usage_purged(); // XXXX this is the old way
   }
}

void ResourceMonitor::fill_sshot_vec_children(const DirState &parent_ds,
                                              int parent_idx,
                                              std::vector<DirStateElement> &vec,
                                              int max_depth)
{
   int pos = vec.size();
   int n_children = parent_ds.m_subdirs.size();

   for (auto const & [name, child] : parent_ds.m_subdirs)
   {
      vec.emplace_back( DirStateElement(child, parent_idx) );
   }

   if (parent_ds.m_depth < max_depth)
   {
      DirStateElement &parent_dse = vec[parent_idx];
      parent_dse.m_daughters_begin = pos;
      parent_dse.m_daughters_end   = pos + n_children;

      for (auto const & [name, child] : parent_ds.m_subdirs)
      {
         if (n_children > 0)
            fill_sshot_vec_children(child, pos, vec, max_depth);
         ++pos;
      }
   }
}

//==============================================================================
// Old prototype from Cache / Purge, now to go into heart_beat() here, above.
//==============================================================================

void Proto_ResourceMonitorHeartBeat()
{
   // static const char *trc_pfx = "ResourceMonitorHeartBeat() ";

   // Pause before initial run
   sleep(1);

   // XXXX Setup initial / constant stats (total RAM, total disk, ???)

   XrdOucCacheStats             &S = Cache::GetInstance().Statistics;
   XrdOucCacheStats::CacheStats &X = S.X;

   S.Lock();

   X.DiskSize = Cache::Conf().m_diskTotalSpace;

   X.MemSize = Cache::Conf().m_RamAbsAvailable;

   S.UnLock();

   // XXXX Schedule initial disk scan, time it!
   //
   // TRACE(Info, trc_pfx << "scheduling intial disk scan.");
   // schedP->Schedule( new ScanAndPurgeJob("XrdPfc::ScanAndPurge") );
   //
   // bool scan_and_purge_running = true;

   // XXXX Could we really hold last-usage for all files in memory?

   // XXXX Think how to handle disk-full, scan/purge not finishing:
   // - start dropping things out of write queue, but only when RAM gets near full;
   // - monitoring this then becomes a high-priority job, inner loop with sleep of,
   //   say, 5 or 10 seconds.

   while (true)
   {
      time_t heartbeat_start = time(0);

      // TRACE(Info, trc_pfx << "HeartBeat starting ...");

      // if sumary monitoring configured, pupulate OucCacheStats:
      S.Lock();

      // - available / used disk space (files usage calculated elsewhere (maybe))

      // - RAM usage
      /* XXXX From Cache
      {  XrdSysMutexHelper lck(&m_RAM_mutex);
         X.MemUsed   = m_RAM_used;
         X.MemWriteQ = m_RAM_write_queue;
      }
      */

      // - files opened / closed etc

      // do estimate of available space
      S.UnLock();

      // if needed, schedule purge in a different thread.
      // purge is:
      // - deep scan + gather FSPurgeState
      // - actual purge
      //
      // this thread can continue running and, if needed, stop writing to disk
      // if purge is taking too long.

      // think how data is passed / synchronized between this and purge thread

      // !!!! think how stat collection is done and propgated upwards;
      // until now it was done once per purge-interval.
      // now stats will be added up more often, but purge will be done
      // only occasionally.
      // also, do we report cumulative values or deltas? cumulative should
      // be easier and consistent with summary data.
      // still, some are state - like disk usage, num of files.

      // Do we take care of directories that need to be newly added into DirState hierarchy?
      // I.e., when user creates new directories and these are covered by either full
      // spec or by root + depth declaration.

      int heartbeat_duration = time(0) - heartbeat_start;

      // TRACE(Info, trc_pfx << "HeartBeat finished, heartbeat_duration " << heartbeat_duration);

      // int sleep_time = m_configuration.m_purgeInterval - heartbeat_duration;
      int sleep_time = 60 - heartbeat_duration;
      if (sleep_time > 0)
      {
         sleep(sleep_time);
      }
   }
}
