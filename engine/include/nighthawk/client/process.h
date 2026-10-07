#pragma once

#include <optional>

#include "nighthawk/client/client_worker.h"
#include "nighthawk/client/output_collector.h"

#include "engine/api/client/output.pb.h"

namespace Nighthawk {
namespace Client {

/**
 * Process context is shared between the CLI and grpc service. It is capable of executing
 * a full Nighthawk test run.
 */
class Process {
public:
  virtual ~Process() = default;

  /**
   * @param collector used to transform output into the desired format.
   * @return bool true iff execution was successfull.
   */
  virtual bool run(OutputCollector& collector) PURE;

  /**
   * Shuts down the worker. Mandatory call before destructing.
   */
  virtual void shutdown() PURE;

  /**
   * Will request all workers to cancel execution asap.
   */
  virtual bool requestExecutionCancellation() PURE;

  /**
   * Snapshots the execution in flight: the live counters and the workers' statistics, as an
   * Output with one "global" result whose execution_duration is the time since the workers
   * started. Safe to call from any thread while run() is in progress.
   *
   * @param detail what the statistics carry: their summaries, which cost nothing to speak of,
   * or full copies with percentiles, which cost a histogram per statistic per worker.
   * @return the snapshot, or nullopt when no workers are running: before they start, after
   * they finish, or when the implementation cannot snapshot.
   */
  virtual std::optional<nighthawk::client::Output> snapshot(SnapshotDetail detail) PURE;
};

using ProcessPtr = std::unique_ptr<Process>;

} // namespace Client
} // namespace Nighthawk
