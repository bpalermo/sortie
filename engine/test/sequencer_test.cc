#include <chrono>
#include <memory>

#include "nighthawk/common/exception.h"
#include "nighthawk/common/platform_util.h"

#include "source/common/event/dispatcher_impl.h"
#include "source/common/stats/isolated_store_impl.h"
#include "test/mocks/event/mocks.h"
#include "test/test_common/simulated_time_system.h"

#include "engine/source/common/rate_limiter_impl.h"
#include "engine/source/common/sequencer_impl.h"
#include "engine/source/common/statistic_impl.h"

#include "engine/test/mocks/common/mock_platform_util.h"
#include "engine/test/mocks/common/mock_rate_limiter.h"
#include "engine/test/mocks/common/mock_termination_predicate.h"

#include "gtest/gtest.h"

using namespace std::chrono_literals;
using namespace nighthawk::client;
using namespace testing;

namespace Nighthawk {

class FakeSequencerTarget {
public:
  virtual ~FakeSequencerTarget() = default;
  // A fake method that matches the sequencer target signature.
  virtual bool callback(OperationCallback) PURE;
};

class MockSequencerTarget : public FakeSequencerTarget {
public:
  MOCK_METHOD(bool, callback, (OperationCallback), (override));
};

class SequencerTestBase : public testing::Test {
public:
  SequencerTestBase()
      : dispatcher_(std::make_unique<Envoy::Event::MockDispatcher>()), frequency_(10_Hz),
        interval_(std::chrono::duration_cast<std::chrono::milliseconds>(frequency_.interval())),
        sequencer_target_(
            std::bind(&SequencerTestBase::callback_test, this, std::placeholders::_1)) {}

  bool callback_test(const OperationCallback& f) {
    callback_test_count_++;
    f(true, true);
    return true;
  }

  MockPlatformUtil platform_util_;
  Envoy::Stats::IsolatedStoreImpl store_;
  Envoy::Stats::Scope& scope_{*store_.rootScope()};
  Envoy::Event::SimulatedTimeSystem time_system_;
  std::unique_ptr<Envoy::Event::MockDispatcher> dispatcher_;
  int callback_test_count_{0};
  const Frequency frequency_;
  const std::chrono::milliseconds interval_;
  const uint64_t test_number_of_intervals_{5};
  SequencerTarget sequencer_target_;
};

class SequencerTest : public SequencerTestBase {
public:
  SequencerTest()
      : rate_limiter_(std::make_unique<MockRateLimiter>()),
        rate_limiter_unsafe_ref_(*rate_limiter_) {}

  std::unique_ptr<MockRateLimiter> rate_limiter_;
  // The sequencers that the tests construct will take ownership of rate_limiter_, we keep a
  // reference, which will become invalid once the sequencer has been destructed.
  MockRateLimiter& rate_limiter_unsafe_ref_;
};

class SequencerTestWithTimerEmulation : public SequencerTest {
public:
  SequencerTestWithTimerEmulation() { setupDispatcherTimerEmulation(); }

  // the Sequencer implementation is effectively driven by two timers. We set us up for emulating
  // those timers firing and moving simulated time forward in simulateTimerloop() below.
  void setupDispatcherTimerEmulation() {
    timer1_ = new NiceMock<Envoy::Event::MockTimer>();
    timer2_ = new NiceMock<Envoy::Event::MockTimer>();
    EXPECT_CALL(*dispatcher_, createTimer_(_))
        .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
          timer_cb_1_ = std::move(cb);
          return timer1_;
        }))
        .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
          timer_cb_2_ = std::move(cb);
          return timer2_;
        }));
    EXPECT_CALL(*timer1_, disableTimer()).WillOnce(Invoke([&]() { timer1_set_ = false; }));
    EXPECT_CALL(*timer2_, disableTimer()).WillOnce(Invoke([&]() { timer2_set_ = false; }));
    EXPECT_CALL(*timer1_, enableHRTimer(_, _))
        .WillRepeatedly(Invoke([&](const std::chrono::microseconds,
                                   const Envoy::ScopeTrackedObject*) { timer1_set_ = true; }));
    EXPECT_CALL(*timer2_, enableHRTimer(_, _))
        .WillRepeatedly(Invoke([&](const std::chrono::microseconds,
                                   const Envoy::ScopeTrackedObject*) { timer2_set_ = true; }));
    EXPECT_CALL(*dispatcher_, exit()).WillOnce(Invoke([&]() { stopped_ = true; }));
    EXPECT_CALL(*dispatcher_, updateApproximateMonotonicTime()).Times(AtLeast(1));
    simulation_start_ = time_system_.monotonicTime();
    auto* unsafe_mock_termination_predicate = new MockTerminationPredicate();
    termination_predicate_ =
        std::unique_ptr<MockTerminationPredicate>(unsafe_mock_termination_predicate);
    EXPECT_CALL(*unsafe_mock_termination_predicate, evaluateChain())
        .WillRepeatedly(Invoke([this]() {
          return (time_system_.monotonicTime() - simulation_start_) <=
                         (test_number_of_intervals_ * interval_)
                     ? TerminationPredicate::Status::PROCEED
                     : TerminationPredicate::Status::TERMINATE;
        }));
  }

  void expectDispatcherRun() {
    EXPECT_CALL(*dispatcher_, run(_))
        .WillOnce(Invoke([&](Envoy::Event::DispatcherImpl::RunType type) {
          ASSERT_EQ(Envoy::Event::DispatcherImpl::RunType::RunUntilExit, type);
          simulateTimerLoop();
        }));
  }

  // Moves time forward 1ms, and runs the ballbacks of set timers.
  void simulateTimerLoop() {
    while (!stopped_) {
      time_system_.setMonotonicTime(time_system_.monotonicTime() + NighthawkTimerResolution);

      // TODO(oschaaf): This can be implemented more accurately, by keeping track of timer
      // enablement preserving ordering of which timer should fire first. For now this seems to
      // suffice for the tests that we have in here.
      if (timer1_set_) {
        timer1_set_ = false;
        timer_cb_1_();
      }

      if (timer2_set_) {
        timer2_set_ = false;
        timer_cb_2_();
      }
    }
  }

  MockSequencerTarget* target() { return &target_; }
  TerminationPredicatePtr termination_predicate_;

protected:
  Envoy::MonotonicTime simulation_start_;

private:
  NiceMock<Envoy::Event::MockTimer>* timer1_; // not owned
  NiceMock<Envoy::Event::MockTimer>* timer2_; // not owned
  Envoy::Event::TimerCb timer_cb_1_;
  Envoy::Event::TimerCb timer_cb_2_;
  MockSequencerTarget target_;
  bool timer1_set_{};
  bool timer2_set_{};
  bool stopped_{};
};

// Basic rate limiter interaction test.
TEST_F(SequencerTestWithTimerEmulation, RateLimiterInteraction) {
  SequencerTarget callback =
      std::bind(&MockSequencerTarget::callback, target(), std::placeholders::_1);
  SequencerImpl sequencer(platform_util_, *dispatcher_, time_system_, std::move(rate_limiter_),
                          callback, std::make_unique<StreamingStatistic>(),
                          std::make_unique<StreamingStatistic>(), SequencerIdleStrategy::SLEEP,
                          std::move(termination_predicate_), scope_);
  // Have the mock rate limiter gate two calls, and block everything else.
  EXPECT_CALL(rate_limiter_unsafe_ref_, tryAcquireOne())
      .Times(AtLeast(3))
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillRepeatedly(Return(false));
  EXPECT_CALL(rate_limiter_unsafe_ref_, elapsed()).Times(2);
  EXPECT_CALL(*target(), callback(_)).Times(2).WillOnce(Return(true)).WillOnce(Return(true));
  expectDispatcherRun();
  EXPECT_CALL(platform_util_, sleep(_)).Times(AtLeast(1));
  sequencer.start();
  sequencer.waitForCompletion();
}

// Saturated rate limiter interaction test.
TEST_F(SequencerTestWithTimerEmulation, RateLimiterSaturatedTargetInteraction) {
  SequencerTarget callback =
      std::bind(&MockSequencerTarget::callback, target(), std::placeholders::_1);
  SequencerImpl sequencer(platform_util_, *dispatcher_, time_system_, std::move(rate_limiter_),
                          callback, std::make_unique<StreamingStatistic>(),
                          std::make_unique<StreamingStatistic>(), SequencerIdleStrategy::SLEEP,
                          std::move(termination_predicate_), scope_);

  EXPECT_CALL(rate_limiter_unsafe_ref_, tryAcquireOne())
      .Times(AtLeast(3))
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillRepeatedly(Return(false));
  EXPECT_CALL(rate_limiter_unsafe_ref_, elapsed()).Times(2);

  EXPECT_CALL(*target(), callback(_)).Times(2).WillOnce(Return(true)).WillOnce(Return(false));

  // The sequencer should call RateLimiter::releaseOne() when the target returns false.
  EXPECT_CALL(rate_limiter_unsafe_ref_, releaseOne());
  expectDispatcherRun();

  EXPECT_CALL(platform_util_, sleep(_)).Times(AtLeast(1));
  sequencer.start();
  sequencer.waitForCompletion();
}

// The integration tests use a LinearRateLimiter.
class SequencerIntegrationTest : public SequencerTestWithTimerEmulation {
public:
  SequencerIntegrationTest() {
    Envoy::Event::SimulatedTimeSystem time_system;
    rate_limiter_ = std::make_unique<LinearRateLimiter>(time_system_, frequency_);
    expectDispatcherRun();
  }

  bool timeout_test(const std::function<void(bool, bool)>& /* f */) {
    callback_test_count_++;
    // We don't call f(); which will cause the sequencer to think there is in-flight work.
    return true;
  }
  bool saturated_test(const std::function<void(bool, bool)>& /* f */) { return false; }

  std::unique_ptr<LinearRateLimiter> rate_limiter_;

  void testRegularFlow(SequencerIdleStrategy::SequencerIdleStrategyOptions idle_strategy) {
    SequencerImpl sequencer(platform_util_, *dispatcher_, time_system_, std::move(rate_limiter_),
                            sequencer_target_, std::make_unique<StreamingStatistic>(),
                            std::make_unique<StreamingStatistic>(), idle_strategy,
                            std::move(termination_predicate_), scope_);
    EXPECT_EQ(0, callback_test_count_);
    EXPECT_EQ(0, sequencer.latencyStatistic().count());
    sequencer.start();
    sequencer.waitForCompletion();
    EXPECT_EQ(test_number_of_intervals_, callback_test_count_);
    EXPECT_EQ(test_number_of_intervals_, sequencer.latencyStatistic().count());
    EXPECT_EQ(0, sequencer.blockedStatistic().count());
    EXPECT_EQ(2, sequencer.statistics().size());
    const auto execution_duration = time_system_.monotonicTime() - simulation_start_;
    EXPECT_EQ(sequencer.executionDuration(), execution_duration);
  }
};

TEST_F(SequencerIntegrationTest, IdleStrategySpin) {
  EXPECT_CALL(platform_util_, yieldCurrentThread()).Times(AtLeast(1));
  EXPECT_CALL(platform_util_, sleep(_)).Times(0);
  testRegularFlow(SequencerIdleStrategy::SPIN);
}

TEST_F(SequencerIntegrationTest, IdleStrategyPoll) {
  EXPECT_CALL(platform_util_, yieldCurrentThread()).Times(0);
  EXPECT_CALL(platform_util_, sleep(_)).Times(0);
  testRegularFlow(SequencerIdleStrategy::POLL);
}

TEST_F(SequencerIntegrationTest, IdleStrategySleep) {
  EXPECT_CALL(platform_util_, yieldCurrentThread()).Times(0);
  EXPECT_CALL(platform_util_, sleep(_)).Times(AtLeast(1));
  testRegularFlow(SequencerIdleStrategy::SLEEP);
}

// Test an always saturated sequencer target. A concrete example would be a http benchmark client
// not being able to start any requests, for example due to misconfiguration or system conditions.
TEST_F(SequencerIntegrationTest, AlwaysSaturatedTargetTest) {
  SequencerTarget callback =
      std::bind(&SequencerIntegrationTest::saturated_test, this, std::placeholders::_1);
  SequencerImpl sequencer(platform_util_, *dispatcher_, time_system_, std::move(rate_limiter_),
                          callback, std::make_unique<StreamingStatistic>(),
                          std::make_unique<StreamingStatistic>(), SequencerIdleStrategy::SLEEP,
                          std::move(termination_predicate_), scope_);
  EXPECT_CALL(platform_util_, sleep(_)).Times(AtLeast(1));
  sequencer.start();
  sequencer.waitForCompletion();

  EXPECT_EQ(0, sequencer.latencyStatistic().count());
  EXPECT_EQ(1, sequencer.blockedStatistic().count());
}

// (SequencerIntegrationTest::timeout_test()) will never call back, effectively simulated a
// stalled benchmark client. Implicitly we test that we get past sequencer.waitForCompletion()
// timely, and don't hang.
TEST_F(SequencerIntegrationTest, CallbacksDoNotInfluenceTestDuration) {
  SequencerTarget callback =
      std::bind(&SequencerIntegrationTest::timeout_test, this, std::placeholders::_1);
  SequencerImpl sequencer(platform_util_, *dispatcher_, time_system_, std::move(rate_limiter_),
                          callback, std::make_unique<StreamingStatistic>(),
                          std::make_unique<StreamingStatistic>(), SequencerIdleStrategy::SLEEP,
                          std::move(termination_predicate_), scope_);
  EXPECT_CALL(platform_util_, sleep(_)).Times(AtLeast(1));
  auto pre_timeout = time_system_.monotonicTime();
  sequencer.start();
  sequencer.waitForCompletion();

  auto diff = time_system_.monotonicTime() - pre_timeout;

  auto expected_duration = (test_number_of_intervals_ * interval_) + NighthawkTimerResolution;
  EXPECT_EQ(expected_duration, diff);

  // the test itself should have seen all callbacks...
  EXPECT_EQ(5, callback_test_count_);
  // ... but they ought to have not arrived at the Sequencer.
  EXPECT_EQ(0, sequencer.latencyStatistic().count());
  EXPECT_EQ(0, sequencer.blockedStatistic().count());
}

// The timer emulation above fires every enabled timer once per 25 microsecond tick, whatever
// delay it was enabled with. That cannot tell the WAIT idle strategy apart from POLL, so this
// fixture honours the delays instead: simulated time jumps to the earliest deadline, and only
// that timer fires. It keeps a record of the delays the periodic timer was armed with.
class SequencerIdleStrategyWaitTest : public SequencerTestBase {
public:
  SequencerIdleStrategyWaitTest() {
    periodic_timer_ = new NiceMock<Envoy::Event::MockTimer>();
    spin_timer_ = new NiceMock<Envoy::Event::MockTimer>();
    // The sequencer creates its periodic timer first.
    EXPECT_CALL(*dispatcher_, createTimer_(_))
        .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
          periodic_cb_ = std::move(cb);
          return periodic_timer_;
        }))
        .WillOnce(Invoke([&](Envoy::Event::TimerCb cb) {
          spin_cb_ = std::move(cb);
          return spin_timer_;
        }));
    EXPECT_CALL(*periodic_timer_, disableTimer()).WillOnce(Invoke([&]() {
      periodic_deadline_.reset();
    }));
    EXPECT_CALL(*spin_timer_, disableTimer()).WillOnce(Invoke([&]() { spin_deadline_.reset(); }));
    EXPECT_CALL(*periodic_timer_, enableHRTimer(_, _))
        .WillRepeatedly(
            Invoke([&](const std::chrono::microseconds delay, const Envoy::ScopeTrackedObject*) {
              periodic_delays_.push_back(delay);
              periodic_deadline_ = time_system_.monotonicTime() + delay + timer_lateness_;
            }));
    EXPECT_CALL(*spin_timer_, enableHRTimer(_, _))
        .WillRepeatedly(
            Invoke([&](const std::chrono::microseconds delay, const Envoy::ScopeTrackedObject*) {
              spin_deadline_ = time_system_.monotonicTime() + delay;
            }));
    EXPECT_CALL(*dispatcher_, exit()).WillOnce(Invoke([&]() { stopped_ = true; }));
    EXPECT_CALL(*dispatcher_, updateApproximateMonotonicTime()).Times(AtLeast(1));
    EXPECT_CALL(*dispatcher_, run(_)).WillOnce(Invoke([&](Envoy::Event::DispatcherImpl::RunType) {
      simulateTimerLoop();
    }));
    // Neither of the other strategies' ways of idling may be used.
    EXPECT_CALL(platform_util_, yieldCurrentThread()).Times(0);
    EXPECT_CALL(platform_util_, sleep(_)).Times(0);
    simulation_start_ = time_system_.monotonicTime();
    auto termination_predicate = std::make_unique<MockTerminationPredicate>();
    EXPECT_CALL(*termination_predicate, evaluateChain()).WillRepeatedly(Invoke([this]() {
      return sinceStart() <= run_for_ ? TerminationPredicate::Status::PROCEED
                                      : TerminationPredicate::Status::TERMINATE;
    }));
    termination_predicate_ = std::move(termination_predicate);
  }

  void simulateTimerLoop() {
    while (!stopped_) {
      ASSERT_TRUE(periodic_deadline_.has_value() || spin_deadline_.has_value())
          << "The sequencer is running without a timer armed, and would hang";
      const bool spin_first =
          spin_deadline_.has_value() &&
          (!periodic_deadline_.has_value() || spin_deadline_.value() <= periodic_deadline_.value());
      std::optional<Envoy::MonotonicTime>& deadline =
          spin_first ? spin_deadline_ : periodic_deadline_;
      time_system_.setMonotonicTime(std::max(time_system_.monotonicTime(), deadline.value()));
      deadline.reset();
      if (spin_first) {
        spin_cb_();
      } else {
        periodic_cb_();
      }
    }
  }

  std::chrono::nanoseconds sinceStart() { return time_system_.monotonicTime() - simulation_start_; }

  // A target that starts and completes a request on the spot, and notes when.
  SequencerTarget recordingTarget() {
    return [this](const OperationCallback& f) {
      release_times_.push_back(sinceStart());
      f(true, true);
      return true;
    };
  }

  std::unique_ptr<SequencerImpl> createSequencer(RateLimiterPtr&& rate_limiter,
                                                 SequencerTarget target) {
    return std::make_unique<SequencerImpl>(
        platform_util_, *dispatcher_, time_system_, std::move(rate_limiter), std::move(target),
        std::make_unique<StreamingStatistic>(), std::make_unique<StreamingStatistic>(),
        SequencerIdleStrategy::WAIT, std::move(termination_predicate_), scope_);
  }

  // What a LinearRateLimiter at frequency_ should release during run_for_.
  std::vector<std::chrono::nanoseconds> expectedLinearReleaseTimes() const {
    std::vector<std::chrono::nanoseconds> expected;
    for (uint64_t i = 0; i < test_number_of_intervals_; i++) {
      expected.push_back((i * interval_) + (interval_ / 2));
    }
    return expected;
  }

  const std::chrono::nanoseconds run_for_{test_number_of_intervals_ * interval_};
  // Added to every delay of the periodic timer, to emulate a timer that fires late.
  std::chrono::microseconds timer_lateness_{0};
  std::vector<std::chrono::microseconds> periodic_delays_;
  std::vector<std::chrono::nanoseconds> release_times_;
  TerminationPredicatePtr termination_predicate_;

private:
  Envoy::MonotonicTime simulation_start_;
  NiceMock<Envoy::Event::MockTimer>* periodic_timer_; // not owned
  NiceMock<Envoy::Event::MockTimer>* spin_timer_;     // not owned
  Envoy::Event::TimerCb periodic_cb_;
  Envoy::Event::TimerCb spin_cb_;
  std::optional<Envoy::MonotonicTime> periodic_deadline_;
  std::optional<Envoy::MonotonicTime> spin_deadline_;
  bool stopped_{};
};

// Requests go out when the rate limiter says they are due, and in between the sequencer wakes up
// once per NighthawkMaxIdleWait rather than once per 25 microseconds.
TEST_F(SequencerIdleStrategyWaitTest, ReleasesAtConfiguredRateWithFewWakeups) {
  auto sequencer = createSequencer(std::make_unique<LinearRateLimiter>(time_system_, frequency_),
                                   recordingTarget());
  sequencer->start();
  sequencer->waitForCompletion();

  const std::vector<std::chrono::nanoseconds> expected = expectedLinearReleaseTimes();
  ASSERT_EQ(expected.size(), release_times_.size());
  for (uint64_t i = 0; i < expected.size(); i++) {
    // The timer has microsecond resolution and the rate limiter computes with doubles, which
    // together can make a request a microsecond late. It is never early.
    EXPECT_GE(release_times_[i], expected[i]);
    EXPECT_LE(release_times_[i], expected[i] + 2us);
  }
  for (const std::chrono::microseconds delay : periodic_delays_) {
    EXPECT_GT(delay, 0us);
    EXPECT_LE(delay, NighthawkMaxIdleWait);
  }
  // One wake-up per capped wait, plus a few around each release. POLL takes 20000 for this run.
  const uint64_t one_per_cap = run_for_ / NighthawkMaxIdleWait;
  EXPECT_GE(periodic_delays_.size(), one_per_cap);
  EXPECT_LE(periodic_delays_.size(), one_per_cap + (4 * test_number_of_intervals_));
  // The termination predicate is evaluated within one capped wait of becoming true.
  EXPECT_GT(sinceStart(), run_for_);
  EXPECT_LE(sinceStart(), run_for_ + NighthawkMaxIdleWait);
  EXPECT_EQ(0, sequencer->blockedStatistic().count());
}

// A timer that fires late, here by more than the interval between two requests, delays requests
// but does not lose any: the rate limiter releases everything that came due in the meantime.
TEST_F(SequencerIdleStrategyWaitTest, LateTimerStillProducesConfiguredRate) {
  timer_lateness_ = 120ms;
  auto sequencer = createSequencer(std::make_unique<LinearRateLimiter>(time_system_, frequency_),
                                   recordingTarget());
  sequencer->start();
  sequencer->waitForCompletion();

  const std::vector<std::chrono::nanoseconds> expected = expectedLinearReleaseTimes();
  ASSERT_EQ(expected.size(), release_times_.size());
  for (uint64_t i = 0; i < expected.size(); i++) {
    EXPECT_GE(release_times_[i], expected[i]);
  }
}

// A rate limiter that cannot tell when it will release is polled at the pace of the POLL
// strategy, so that nothing depends on every rate limiter being able to tell.
TEST_F(SequencerIdleStrategyWaitTest, UnknownTimeUntilNextReleaseFallsBackToPolling) {
  auto rate_limiter = std::make_unique<NiceMock<MockRateLimiter>>();
  EXPECT_CALL(*rate_limiter, tryAcquireOne())
      .WillOnce(Return(false))
      .WillOnce(Return(false))
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillRepeatedly(Return(false));
  EXPECT_CALL(*rate_limiter, timeUntilNextRelease())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(std::nullopt));
  auto sequencer = createSequencer(std::move(rate_limiter), recordingTarget());
  sequencer->start();
  sequencer->waitForCompletion();

  EXPECT_EQ(2, release_times_.size());
  ASSERT_FALSE(periodic_delays_.empty());
  for (const std::chrono::microseconds delay : periodic_delays_) {
    EXPECT_EQ(delay, NighthawkTimerResolution);
  }
}

// However long the rate limiter says the wait is, the sequencer keeps running often enough to
// notice promptly that it has to stop.
TEST_F(SequencerIdleStrategyWaitTest, LongWaitIsCappedSoTerminationStaysPrompt) {
  auto rate_limiter = std::make_unique<NiceMock<MockRateLimiter>>();
  EXPECT_CALL(*rate_limiter, tryAcquireOne()).WillRepeatedly(Return(false));
  EXPECT_CALL(*rate_limiter, timeUntilNextRelease())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(std::chrono::nanoseconds(1h)));
  auto sequencer = createSequencer(std::move(rate_limiter), recordingTarget());
  sequencer->start();
  sequencer->waitForCompletion();

  EXPECT_TRUE(release_times_.empty());
  for (const std::chrono::microseconds delay : periodic_delays_) {
    EXPECT_EQ(delay, NighthawkMaxIdleWait);
  }
  EXPECT_GT(sinceStart(), run_for_);
  EXPECT_LE(sinceStart(), run_for_ + NighthawkMaxIdleWait);
}

// Closed-loop mode: a target without capacity makes the sequencer hand the acquisition back, after
// which the rate limiter reports that it would release right away. The sequencer must neither
// spin on that nor sleep through the moment the target has room again, and the requests that
// were held back must all still go out.
TEST_F(SequencerIdleStrategyWaitTest, BlockedTargetIsRetriedWithoutSpinning) {
  const std::chrono::nanoseconds blocked_until = 2 * interval_;
  uint64_t refusals = 0;
  SequencerTarget target = [&](const OperationCallback& f) {
    if (sinceStart() < blocked_until) {
      refusals++;
      return false;
    }
    release_times_.push_back(sinceStart());
    f(true, true);
    return true;
  };
  auto sequencer =
      createSequencer(std::make_unique<LinearRateLimiter>(time_system_, frequency_), target);
  sequencer->start();
  sequencer->waitForCompletion();

  EXPECT_GT(refusals, 1);
  for (const std::chrono::microseconds delay : periodic_delays_) {
    EXPECT_GT(delay, 0us);
    EXPECT_LE(delay, NighthawkMaxIdleWait);
  }
  // Nothing was dropped: the two requests that came due while blocked went out when the target
  // had room, the first of them within one poll of that moment.
  const std::vector<std::chrono::nanoseconds> expected = expectedLinearReleaseTimes();
  ASSERT_EQ(expected.size(), release_times_.size());
  EXPECT_GE(release_times_[0], blocked_until);
  EXPECT_LE(release_times_[0], blocked_until + NighthawkTimerResolution);
  EXPECT_EQ(release_times_[1], release_times_[0]);
  for (uint64_t i = 2; i < expected.size(); i++) {
    EXPECT_GE(release_times_[i], expected[i]);
    EXPECT_LE(release_times_[i], expected[i] + 2us);
  }
  EXPECT_EQ(1, sequencer->blockedStatistic().count());
}

} // namespace Nighthawk
