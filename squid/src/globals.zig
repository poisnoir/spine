const protocol = @import("protocol");

// Shared spine wire protocol (command codes, status codes, peer types, and
// the MAX_* array bounds GetInfoResponse relies on) lives in
// protocol/src/globals.zig, so squid and spined can't drift apart on what
// these numbers mean. Re-exported here so the rest of this package can keep
// saying `globals.FOO` for both the shared and squid-only constants below.
// squid-only settings.
pub const BUF_SIZE = 256;

// CLI tokens
pub const VERB_ADD = "add";
pub const VERB_INFO = "info";
pub const NOUN_NAMESPACE = "namespace";
