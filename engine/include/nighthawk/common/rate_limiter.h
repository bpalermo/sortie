#pragma once

#include <chrono>
#include <memory>
#include <optional>

#include "envoy/common/pure.h"
#include "envoy/common/time.h"

namespace Nighthawk {

/**
 * Abstract rate limiter interface.
 */
class RateLimiter {
public:
  virtual ~RateLimiter() = default;

  /**
   * Acquire a controlled resource.
   * @return true Indicates success.
   * @return false Indicates failure to acquire.
   */
  virtual bool tryAcquireOne() PURE;

  /**
   * Releases a controlled resource.
   */
  virtual void releaseOne() PURE;

  /**
   * @return Envoy::TimeSource& time_source used to track time.
   */
  virtual Envoy::TimeSource& timeSource() PURE;

  /**
   * @return std::optional<Envoy::SystemTime> Time of the first acquisition, if any.
   */
  virtual std::optional<Envoy::SystemTime> firstAcquisitionTime() const PURE;

  /**
   * @return std::chrono::nanoseconds elapsed since the first call to tryAcquireOne(). Used by some
   * rate limiter implementations to compute acquisition rate.
   */
  virtual std::chrono::nanoseconds elapsed() PURE;

  /**
   * Tells a caller that just saw tryAcquireOne() fail how long it can wait before trying again.
   *
   * The value is a lower bound: tryAcquireOne() will not succeed sooner, but it may still fail
   * once the time has passed (a filter may suppress the acquisition, a burst may still be
   * accumulating). The caller is expected to call tryAcquireOne() again when the time is up, and
   * to ask again if that fails. A lower bound is what makes this safe to forward through any
   * wrapper: waking up early costs a wake-up, waking up late costs pacing accuracy.
   *
   * Zero means "try now". std::nullopt means the limiter cannot tell, and the caller has to
   * poll. That is the default, so that a limiter which predates this method (a plugin, say) keeps
   * working unchanged.
   *
   * @return std::optional<std::chrono::nanoseconds> a lower bound on the time until
   * tryAcquireOne() can succeed, or std::nullopt when that is unknown.
   */
  virtual std::optional<std::chrono::nanoseconds> timeUntilNextRelease() { return std::nullopt; }
};

using RateLimiterPtr = std::unique_ptr<RateLimiter>;

/**
 * Interface to sample discrete numeric distributions.
 */
class DiscreteNumericDistributionSampler {
public:
  virtual ~DiscreteNumericDistributionSampler() = default;
  /**
   * @return uint64_t gets a sample value from the distribution.
   */
  virtual uint64_t getValue() PURE;
  /**
   * @return uint64_t minimum sample value that can be returned by getValue().
   */
  virtual uint64_t min() const PURE;
  /**
   * @return uint64_t maximum sample value that can returned by getValue().
   */
  virtual uint64_t max() const PURE;
};

using DiscreteNumericDistributionSamplerPtr = std::unique_ptr<DiscreteNumericDistributionSampler>;

} // namespace Nighthawk
