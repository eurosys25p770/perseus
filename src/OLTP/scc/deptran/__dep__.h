#pragma once

//C++ standard library
#include "constants.h"
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>
#include <vector>
#include <list>
#include <chrono>
#include <thread>
#include <iostream>
#include <condition_variable>
#include <atomic>
#include <cstdint>
#include <algorithm>
#include <set>
#include <functional>
#include <bitset>
using std::map;
using std::unordered_map;
using std::string;
using std::vector;
using std::list;
using std::set;
using std::unordered_set;
using std::pair;
using std::function;

// system library
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>


// boost library
#include <boost/any.hpp>
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

// yaml-cpp
#include <yaml-cpp/yaml.h>

// c library
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cinttypes>

// google library

// misc helper files
#include "rrr.hpp"

using namespace rrr;

#define deptran rococo

using rrr::NoCopy;
using rrr::Log;
using rrr::i8;
using rrr::i16;
using rrr::i32;
using rrr::i64;
using rrr::Mutex;
using rrr::Future;
using rrr::DragonBall;
using rrr::RandomGenerator;
using rrr::Recorder;
using rrr::AvgStat;
using rrr::ALock;
using rrr::TimeoutALock;
using rrr::ScopedLock;
using rrr::PollMgr;
using rrr::Marshal;

// User include files
//

#include "memdb/value.h"
#include "memdb/schema.h"
#include "memdb/table.h"
#include "memdb/txn.h"
#include "memdb/txn_2pl.h"
#include "memdb/txn_occ.h"
#include "memdb/txn_unsafe.h"
#include "memdb/utils.h"
#include "memdb/row.h"
#include "deptran/marshal-value.h"
#include "deptran/chronos/types.h"
using mdb::Value;
using mdb::Row;
using mdb::VersionedRow;
using mdb::symbol_t;
using mdb::Table;
using mdb::column_id_t;
using mdb::SnapshotTable;


// rpc library
class dummy_class {
 public:
  dummy_class() {
#ifdef LOG_LEVEL_AS_DEBUG
    Log::set_level(Log::DEBUG);
#else
    Log::set_level(Log::INFO);
//    Log::set_level(Log::WARN);
#endif
  }
};
static dummy_class dummy___;

typedef int32_t varid_t;

typedef map<innid_t, map<varid_t, Value>> TxnOutput;

//Some macros defined by xs for debugging chronos
//
//#define LAT_BREAKDOWN


//#define DEP_DEBUGGING_ON

#ifdef DEP_DEBUGGING_ON
#define DEP_LOG(x...) Log_debug(x)
#else
#define DEP_LOG(x...)
#endif

//#define SYNC_DEBUGGING_ON

#ifdef SYNC_DEBUGGING_ON
#define SYNC_LOG(x...) Log_debug(x)
#else
#define SYNC_LOG(x...)
#endif

//#define SERIAL_DEBUGGING_ON

#ifdef SERIAL_DEBUGGING_ON
#define SER_LOG(x...) Log_debug(x)
#else
#define SER_LOG(x...)
#endif

//#define RPC_DEBUGGING_ON

#ifdef RPC_DEBUGGING_ON
#define RPC_LOG(x...) Log_debug(x)
#else
#define RPC_LOG(x...)
#endif
