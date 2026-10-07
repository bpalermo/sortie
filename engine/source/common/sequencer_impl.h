#pragma once

#include "envoy/common/pure.h"
#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"

#include "nighthawk/common/platform_util.h"
#include "nighthawk/common/rate_limiter.h"
#include "nighthawk/common/sequencer.h"
#include "nighthawk/common/statistic.h"
#include "nighthawk/common/termination_predicate.h"

#include "source/common/common/logger.h"

namespace Nighthawk {

namespace {

using namespace std::chrono_literals;

// We shoot for a 40kHz resolution.
constexpr std::chrono::microseconds NighthawkTimerResolution = 25us;

// The longest the WAIT idle strategy lets the sequencer go without running. Besides releasing
// requests, running is how the sequencer evaluates the termination predicates: the duration, the
// failure predicates, and a cancellation, which arrives as a counter that one of them watches.
// The cap is therefore how late an execution can end or notice it was cancelled, and by how much
// it can overrun its duration. No request is sent in that overrun, as the predicates are
// evaluated first. It also bounds the damage should a rate limiter report a wait that is too
// long.
//
// The cap is a trade against idle cost, which is all wake-ups, and most of a wake-up that finds
// nothing to do is spent in the kernel. Measured on an optimized build, a worker with next to
// nothing to send costs 27 millicores when capped at 1 ms and 11 when capped at 5 ms. The
// difference is not worth ending a run 4 ms sooner.
constexpr std::chrono::microseconds NighthawkMaxIdleWait = 5ms;

} // namespace

#define ALL_SEQUENCER_STATS(COUNTER) COUNTER(failed_terminations)

struct SequencerStats {
  ALL_SEQUENCER_STATS(GENERATE_COUNTER_STRUCT)
};

/**
 * The Sequencer will drive calls to the SequencerTarget at a pace indicated by the associated
 * RateLimiter. The contract with the target is that it will call the provided callback when it is
 * ready. The target will return true if it was able to proceed, or false if a retry is warranted at
 * a later time (because of being out of required resources, for example).
 * Note that owner of SequencerTarget must outlive the SequencerImpl to avoid use-after-free.
 * Also, the Sequencer implementation is a single-shot design. The general usage pattern is:
 *   SequencerImpl sequencer(...)
 *   sequencer.start();
 *   sequencer.waitForCompletion();
 */
class SequencerImpl : public Sequencer, public Envoy::Logger::Loggable<Envoy::Logger::Id::main> {
public:
  SequencerImpl(
      const PlatformUtil& platform_util, Envoy::Event::Dispatcher& dispatcher,
      Envoy::TimeSource& time_source, RateLimiterPtr&& rate_limiter, SequencerTarget target,
      StatisticPtr&& latency_statistic, StatisticPtr&& blocked_statistic,
      nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions idle_strategy,
      TerminationPredicatePtr&& termination_predicate, Envoy::Stats::Scope& scope);

  /**
   * Starts the Sequencer. Should be followed up with a call to waitForCompletion().
   */
  void start() override;

  /**
   * Blocking call that waits for the Sequencer flow to terminate. Start() must have been called
   * before this.
   */
  void waitForCompletion() override;

  std::chrono::nanoseconds executionDuration() const override { return rate_limiter_->elapsed(); }

  const RateLimiter& rate_limiter() const override { return *rate_limiter_; }

  double completionsPerSecond() const override {
    const double usec =
        std::chrono::duration_cast<std::chrono::microseconds>(executionDuration()).count();

    return usec == 0 ? 0 : ((targets_completed_ / usec) * 1000000);
  }

  StatisticPtrMap statistics() const override;

  const Statistic& blockedStatistic() const { return *blocked_statistic_; }
  const Statistic& latencyStatistic() const { return *latency_statistic_; }

protected:
  /**
   * Run is called initially by start() and thereafter by two timers:
   *  - a periodic one running at a 1 ms resolution (the current minimum)
   *  - one to spin on calls to run().
   *
   * Spinning is performed when the Sequencer implementation considers itself idle, where "idle" is
   * defined as:
   * - All benchmark target calls have reported back
   * - Either the rate limiter or the benchmark target is prohibiting initiation of the next
   * benchmark target call.
   *
   * The spinning is performed to improve timelyness when initiating
   * calls to the benchmark targets, and observational data also shows significant improvement of
   * actually latency measurement (most pronounced on non-tuned systems). As a side-effect, spinning
   * keeps the CPU busy, preventing C-state frequency changes. Systems with appropriately cooled
   * processors should not be impacted by thermal throttling. When thermal throttling does occur, it
   * makes sense to first warm up the system to get it into a steady state regarding processor
   * frequency.
   *
   * For more context on the current implementation of how we spin, see the the review discussion:
   * https://github.com/envoyproxy/envoy-perf/pull/49#discussion_r259133387
   *
   * With the WAIT idle strategy none of that spinning happens. The periodic timer is armed for
   * the time the rate limiter says is left until the next request instead, see idleWait().
   *
   * @param from_periodic_timer Indicates if we this is called from the periodic timer.
   * Used to determine if re-enablement of the periodic timer should be performed before returning.
   */
  void run(bool from_periodic_timer);
  void scheduleRun();
  /**
   * @return std::chrono::microseconds how long the periodic timer may wait before the next run
   * under the WAIT idle strategy.
   */
  std::chrono::microseconds idleWait();
  void stop(bool timed_out);
  void unblockAndUpdateStatisticIfNeeded(const Envoy::MonotonicTime& now);
  void updateStartBlockingTimeIfNeeded();

private:
  SequencerTarget target_;
  const PlatformUtil& platform_util_;
  Envoy::Event::Dispatcher& dispatcher_;
  Envoy::TimeSource& time_source_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  StatisticPtr latency_statistic_;
  StatisticPtr blocked_statistic_;
  Envoy::Event::TimerPtr periodic_timer_;
  Envoy::Event::TimerPtr spin_timer_;
  uint64_t targets_initiated_{0};
  uint64_t targets_completed_{0};
  bool running_{};
  bool blocked_{};
  Envoy::MonotonicTime blocked_start_;
  nighthawk::client::SequencerIdleStrategy::SequencerIdleStrategyOptions idle_strategy_;
  TerminationPredicatePtr termination_predicate_;
  TerminationPredicate::Status last_termination_status_;
  Envoy::Stats::ScopeSharedPtr scope_;
  SequencerStats sequencer_stats_;
};

} // namespace Nighthawk
