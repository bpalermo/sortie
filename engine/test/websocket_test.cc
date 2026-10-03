#include "source/common/buffer/buffer_impl.h"

#include "engine/source/common/websocket.h"

#include "gtest/gtest.h"

namespace Nighthawk {
namespace WebSocket {
namespace {

// RFC 6455 section 4.2.2's worked example.
TEST(WebSocketTest, AcceptKeyMatchesTheRfcExample) {
  EXPECT_EQ("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", acceptKey("dGhlIHNhbXBsZSBub25jZQ=="));
}

TEST(WebSocketTest, NewKeysAreBase64Of16BytesAndDiffer) {
  const std::string a = newKey();
  const std::string b = newKey();
  EXPECT_EQ(24, a.size());
  EXPECT_EQ('=', a[23]);
  EXPECT_NE(a, b);
}

TEST(WebSocketTest, UnmaskedFramesRoundTripAtEveryLengthClass) {
  for (const size_t length : {size_t(0), size_t(5), size_t(125), size_t(126), size_t(65535),
                              size_t(65536), size_t(100000)}) {
    Frame frame;
    frame.opcode = Opcode::Binary;
    frame.payload = std::string(length, 'x');
    const std::string bytes = encodeFrame(frame, /*mask=*/false);
    EXPECT_EQ(0x82, static_cast<uint8_t>(bytes[0]));
    Envoy::Buffer::OwnedImpl buffer(bytes);
    std::vector<Frame> frames;
    ASSERT_TRUE(Decoder().feed(buffer, frames));
    ASSERT_EQ(1, frames.size());
    EXPECT_TRUE(frames[0].fin);
    EXPECT_EQ(Opcode::Binary, frames[0].opcode);
    EXPECT_EQ(frame.payload, frames[0].payload);
    EXPECT_EQ(0, buffer.length());
  }
}

TEST(WebSocketTest, MaskedFramesAreMaskedOnTheWireAndDecodeToThePayload) {
  Frame frame;
  frame.opcode = Opcode::Text;
  frame.payload = "hello";
  const std::string bytes = encodeFrame(frame, /*mask=*/true);
  ASSERT_EQ(2 + 4 + 5, bytes.size());
  EXPECT_EQ(0x81, static_cast<uint8_t>(bytes[0]));
  EXPECT_EQ(0x80 | 5, static_cast<uint8_t>(bytes[1]));
  // The payload on the wire differs from the plaintext unless the mask happens to be zero.
  Envoy::Buffer::OwnedImpl buffer(bytes);
  std::vector<Frame> frames;
  ASSERT_TRUE(Decoder().feed(buffer, frames));
  ASSERT_EQ(1, frames.size());
  EXPECT_EQ("hello", frames[0].payload);
}

TEST(WebSocketTest, DecoderLeavesAPartialFrameForLater) {
  Frame frame;
  frame.payload = "0123456789";
  const std::string bytes = encodeFrame(frame, /*mask=*/false);
  Envoy::Buffer::OwnedImpl buffer;
  std::vector<Frame> frames;
  Decoder decoder;
  buffer.add(bytes.substr(0, 1));
  ASSERT_TRUE(decoder.feed(buffer, frames));
  EXPECT_TRUE(frames.empty());
  buffer.add(bytes.substr(1, 6));
  ASSERT_TRUE(decoder.feed(buffer, frames));
  EXPECT_TRUE(frames.empty());
  EXPECT_EQ(7, buffer.length());
  buffer.add(bytes.substr(7));
  // Two frames back to back decode in one call.
  buffer.add(encodeFrame(closeFrame(1000), /*mask=*/false));
  ASSERT_TRUE(decoder.feed(buffer, frames));
  ASSERT_EQ(2, frames.size());
  EXPECT_EQ("0123456789", frames[0].payload);
  EXPECT_EQ(Opcode::Close, frames[1].opcode);
  EXPECT_EQ(std::string("\x03\xe8", 2), frames[1].payload);
  EXPECT_EQ(0, buffer.length());
}

// A 64-bit length with the high bit set, or merely enormous, is a malformed header: rejected
// before any arithmetic on it can wrap or any buffer can be sized by it.
TEST(WebSocketTest, DecoderRejectsImpossiblePayloadLengths) {
  for (const std::string& length : {std::string("\xff\xff\xff\xff\xff\xff\xff\xff", 8),
                                   std::string("\x80\x00\x00\x00\x00\x00\x00\x00", 8),
                                   std::string("\x00\x00\x00\x00\x10\x00\x00\x00", 8)}) {
    Envoy::Buffer::OwnedImpl buffer(std::string("\x82\x7f", 2) + length);
    std::vector<Frame> frames;
    EXPECT_FALSE(Decoder().feed(buffer, frames));
    EXPECT_EQ(0, buffer.length());
    EXPECT_TRUE(frames.empty());
  }
}

TEST(WebSocketTest, DecoderRejectsReservedOpcodesAndFragmentedControlFrames) {
  {
    Envoy::Buffer::OwnedImpl buffer(std::string("\x83\x00", 2)); // opcode 3: reserved
    std::vector<Frame> frames;
    EXPECT_FALSE(Decoder().feed(buffer, frames));
    EXPECT_EQ(0, buffer.length());
  }
  {
    Envoy::Buffer::OwnedImpl buffer(std::string("\x09\x00", 2)); // ping without FIN
    std::vector<Frame> frames;
    EXPECT_FALSE(Decoder().feed(buffer, frames));
  }
  {
    Envoy::Buffer::OwnedImpl buffer(std::string("\x80\x00", 2)); // continuation, fine
    std::vector<Frame> frames;
    EXPECT_TRUE(Decoder().feed(buffer, frames));
    ASSERT_EQ(1, frames.size());
    EXPECT_EQ(Opcode::Continuation, frames[0].opcode);
  }
}

} // namespace
} // namespace WebSocket
} // namespace Nighthawk
