# Protocol compatibility distributions

These are maintained distributions of the published Engineioxide 0.17.7 and
Socketioxide 0.18.7 crates from https://github.com/Totodore/socketioxide,
upstream revision 708ea9fc5ee549229d7236924e4975ec5977a59c. Original MIT
licenses and author attribution remain in each package.

Breeze publishes brz-engineioxide 0.17.7-brz.1 and brz-socketioxide
0.18.7-brz.1; Rust library names and native APIs remain unchanged.
Socketioxide changes package identity, its Engineioxide dependency and test
harness feature declarations. Harness helpers are available during unit tests;
integration tests and benchmarks declare their required features explicitly.
Shared core/parser and Redis adapter packages remain upstream packages, so
their adapter traits agree.

Engineioxide changes:
- Wait the advertised interval before the first Engine.IO v4 PING.
- Send a normal (1000), empty-reason WebSocket Close on server closure.
  Tungstenite preserves a received client's explicit Close reply.
- WebSocket heartbeat expiry closes the transport without injecting an Engine.IO
  CLOSE data packet, matching python-engineio's timeout path. Polling expiry and
  explicit server closure retain their Engine.IO CLOSE packet.

These are registry dependencies: a patch inside a published SDK does not
propagate to applications. SDK tests cover real WebSocket NoPong expiry,
ongoing PONG liveness and preservation of a client's close status.
Publishing verifies and uploads protocol packages before the SDK.

Protocol tests retain the advertised interval plus timeout in their deadlines;
polling packet-order tests no longer try to drain an immediate initial PING.

Exact original crates.io archive SHA-256 values:

- engineioxide 0.17.7: 0235543b6e1a5fef2a8a7871ae410d5d1c440d8ced9217ec0f1b344edfe30673
- socketioxide 0.18.7: 6d23ebfa62e73bcc05c40301f8b4cb8400489400c5e794e789300a04fd28a3b8
