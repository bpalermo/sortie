#include "engine/source/common/websocket.h"

#include "source/common/common/base64.h"
#include "source/common/common/random_generator.h"
#include "source/common/common/safe_memcpy.h"

#include "openssl/sha.h"

namespace Nighthawk {
namespace WebSocket {

namespace {

constexpr absl::string_view kGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
// The largest payload a frame may announce. Far below the 2^63-1 the wire format allows: a
// message this engine sends or expects echoed is kilobytes, and anything claiming more is a
// broken or hostile peer, not a frame to buffer.
constexpr uint64_t kMaxPayloadLength = 64 * 1024 * 1024;

bool isControl(Opcode opcode) { return static_cast<uint8_t>(opcode) >= 0x8; }

bool isKnown(uint8_t opcode) {
  switch (static_cast<Opcode>(opcode)) {
  case Opcode::Continuation:
  case Opcode::Text:
  case Opcode::Binary:
  case Opcode::Close:
  case Opcode::Ping:
  case Opcode::Pong:
    return true;
  }
  return false;
}

void applyMask(std::string& payload, const uint8_t key[4]) {
  for (size_t i = 0; i < payload.size(); i++) {
    payload[i] = static_cast<char>(static_cast<uint8_t>(payload[i]) ^ key[i % 4]);
  }
}

} // namespace

std::string acceptKey(absl::string_view key) {
  const std::string input = std::string(key) + std::string(kGuid);
  uint8_t digest[SHA_DIGEST_LENGTH];
  SHA1(reinterpret_cast<const uint8_t*>(input.data()), input.size(), digest);
  return Envoy::Base64::encode(reinterpret_cast<const char*>(digest), SHA_DIGEST_LENGTH);
}

std::string newKey() {
  Envoy::Random::RandomGeneratorImpl random;
  uint64_t words[2] = {random.random(), random.random()};
  return Envoy::Base64::encode(reinterpret_cast<const char*>(words), sizeof(words));
}

std::string encodeFrame(const Frame& frame, bool mask) {
  std::string out;
  out.reserve(frame.payload.size() + 14);
  out.push_back(static_cast<char>((frame.fin ? 0x80 : 0x00) | static_cast<uint8_t>(frame.opcode)));
  const uint64_t length = frame.payload.size();
  const uint8_t mask_bit = mask ? 0x80 : 0x00;
  if (length < 126) {
    out.push_back(static_cast<char>(mask_bit | length));
  } else if (length <= 0xFFFF) {
    out.push_back(static_cast<char>(mask_bit | 126));
    out.push_back(static_cast<char>(length >> 8));
    out.push_back(static_cast<char>(length & 0xFF));
  } else {
    out.push_back(static_cast<char>(mask_bit | 127));
    for (int shift = 56; shift >= 0; shift -= 8) {
      out.push_back(static_cast<char>((length >> shift) & 0xFF));
    }
  }
  if (!mask) {
    out.append(frame.payload);
    return out;
  }
  Envoy::Random::RandomGeneratorImpl random;
  const uint32_t word = static_cast<uint32_t>(random.random());
  uint8_t key[4];
  Envoy::safeMemcpy(&key, &word);
  out.append(reinterpret_cast<const char*>(key), sizeof(key));
  std::string payload = frame.payload;
  applyMask(payload, key);
  out.append(payload);
  return out;
}

Frame closeFrame(uint16_t code) {
  Frame frame;
  frame.opcode = Opcode::Close;
  frame.payload.push_back(static_cast<char>(code >> 8));
  frame.payload.push_back(static_cast<char>(code & 0xFF));
  return frame;
}

bool Decoder::feed(Envoy::Buffer::Instance& data, std::vector<Frame>& frames) {
  while (data.length() >= 2) {
    uint8_t header[2];
    data.copyOut(0, 2, header);
    const bool fin = (header[0] & 0x80) != 0;
    const uint8_t rsv = header[0] & 0x70;
    const uint8_t opcode = header[0] & 0x0F;
    const bool masked = (header[1] & 0x80) != 0;
    uint64_t length = header[1] & 0x7F;
    uint64_t header_length = 2;
    if (rsv != 0 || !isKnown(opcode)) {
      data.drain(data.length());
      return false;
    }
    if (length == 126) {
      header_length += 2;
    } else if (length == 127) {
      header_length += 8;
    }
    if (masked) {
      header_length += 4;
    }
    if (data.length() < header_length) {
      return true;
    }
    uint8_t extended[8];
    if (length == 126) {
      data.copyOut(2, 2, extended);
      length = (static_cast<uint64_t>(extended[0]) << 8) | extended[1];
    } else if (length == 127) {
      data.copyOut(2, 8, extended);
      length = 0;
      for (int i = 0; i < 8; i++) {
        length = (length << 8) | extended[i];
      }
      // The high bit must be clear (RFC 6455 5.2), and a length anywhere near it would be a
      // malformed header asking for an impossible allocation.
      if (length > kMaxPayloadLength) {
        data.drain(data.length());
        return false;
      }
    }
    const Opcode op = static_cast<Opcode>(opcode);
    if (isControl(op) && (!fin || length > 125)) {
      data.drain(data.length());
      return false;
    }
    if (data.length() < header_length + length) {
      return true;
    }
    uint8_t key[4] = {0, 0, 0, 0};
    if (masked) {
      data.copyOut(header_length - 4, 4, key);
    }
    Frame frame;
    frame.fin = fin;
    frame.opcode = op;
    frame.payload.resize(length);
    if (length > 0) {
      data.copyOut(header_length, length, frame.payload.data());
    }
    if (masked) {
      applyMask(frame.payload, key);
    }
    data.drain(header_length + length);
    frames.push_back(std::move(frame));
  }
  return true;
}

} // namespace WebSocket
} // namespace Nighthawk
