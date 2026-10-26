// Exercise the official Socket.IO client against the standalone echo example.
import assert from 'node:assert/strict'
import { once } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import { io } from 'socket.io-client'

const base = process.env.SOCKET_IO_TEST_URL || 'http://127.0.0.1:3000'
const path = process.env.SOCKET_IO_TEST_PATH || '/socket.io'
const namespace = process.env.SOCKET_IO_TEST_NAMESPACE || '/'
const prefix = process.env.SOCKET_IO_TEST_ACK_PREFIX || ''
const envelope = process.env.SOCKET_IO_TEST_ACK_ENVELOPE === '1'
const expected = value => envelope ? { ok: true, data: `${prefix}${value}` } : `${prefix}${value}`
const deadline = AbortSignal.timeout(5000)
while (true) {
  try {
    const response = await fetch(`${base}${path}/?EIO=4&transport=polling`, { signal: deadline })
    assert.equal(response.status, 200)
    break
  } catch (error) {
    if (deadline.aborted) throw error
    await sleep(20)
  }
}

for (const transports of [['websocket'], ['polling'], ['polling', 'websocket']]) {
  const socket = io(namespace === '/' ? base : `${base}${namespace}`, { path, transports, reconnection: false, autoConnect: false, timeout: 3000 })
  try {
    const connected = once(socket, 'connect', { signal: AbortSignal.timeout(5000) })
    socket.connect()
    await connected
    assert.deepEqual(await socket.timeout(3000).emitWithAck('echo', 'official-client'), expected('official-client'))
    if (transports.length === 2 && socket.io.engine.transport.name !== 'websocket') {
      await once(socket.io.engine, 'upgrade', { signal: AbortSignal.timeout(5000) })
    }
    assert.equal(socket.io.engine.transport.name, transports.at(-1))
    assert.deepEqual(await socket.timeout(3000).emitWithAck('echo', 'after-upgrade'), expected('after-upgrade'))
    console.log(`passed: ${transports.join(' -> ')} with ACK`)
  } finally {
    socket.disconnect()
  }
}

if (process.env.SOCKET_IO_TEST_HTTP_HEALTH) {
  const response = await fetch(`${base}${process.env.SOCKET_IO_TEST_HTTP_HEALTH}`, { signal: AbortSignal.timeout(5000) })
  assert.equal(response.status, 200)
  assert.equal(await response.text(), 'ok')
  console.log('passed: HTTP API on the Socket.IO listener')
}
