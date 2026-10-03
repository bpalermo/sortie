#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "envoy/buffer/buffer.h"

#include "absl/strings/string_view.h"

namespace Nighthawk {
namespace WebSocket {

/**
 * The parts of RFC 6455 the engine needs on both ends: the handshake's accept key, and framing.
 * Extensions (compression) are not negotiated and not supported.
 */
enum class Opcode : uint8_t {
  Continuation = 0x0,
  Text = 0x1,
  Binary = 0x2,
  Close = 0x8,
  Ping = 0x9,
  Pong = 0xA,
};

struct Frame {
  bool fin{true};
  Opcode opcode{Opcode::Binary};
  std::string payload;
};

/**
 * @param key the request's Sec-WebSocket-Key.
 * @return std::string the Sec-WebSocket-Accept a server must answer with (RFC 6455 4.2.2).
 */
std::string acceptKey(absl::string_view key);

/**
 * @return std::string a fresh Sec-WebSocket-Key: 16 random bytes, base64 (RFC 6455 4.1).
 */
std::string newKey();

/**
 * Encodes one frame.
 *
 * @param frame the frame.
 * @param mask whether to mask the payload with a random key, which a client must and a server
 * must not (RFC 6455 5.3).
 * @return std::string the frame's bytes.
 */
std::string encodeFrame(const Frame& frame, bool mask);

/**
 * @param code the close status code (RFC 6455 7.4).
 * @return Frame a Close frame carrying it.
 */
Frame closeFrame(uint16_t code);

/**
 * Incremental frame decoder. Bytes that do not yet make a whole frame are left in the buffer for
 * the next call.
 */
class Decoder {
public:
  /**
   * Consumes every complete frame at the front of data.
   *
   * @param data the bytes received so far; whole frames are drained from it.
   * @param frames receives the decoded frames, payloads unmasked.
   * @return bool false on a protocol error (a reserved opcode, a control frame that is
   * fragmented or oversized, a payload length over 64 MiB or with the forbidden high bit set),
   * after which data has been drained and the connection should be closed.
   */
  bool feed(Envoy::Buffer::Instance& data, std::vector<Frame>& frames);
};

} // namespace WebSocket
} // namespace Nighthawk
