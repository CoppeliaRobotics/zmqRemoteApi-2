"""
ZMQRemoteAPI – asynchronous, bidirectional RPC over ZMQ DEALER sockets.

Architecture overview:
----------------------
Fully asynchronous, request‑ID driven design. Both client and server use
DEALER sockets, allowing either side to send a request (call) at any time,
and responses are matched by a unique message ID.

Key features:
    * No strict send/recv alternation – messages can flow in any order.
    * Re‑entrant callbacks: while processing a call, the remote side can invoke
      a callback, and that callback can itself invoke another callback, etc.
    * Both sides can register callbacks (registerCallback) which, when invoked
      on the remote side, will transparently call back to the local side.
    * The `call()` method blocks until its response arrives, but it processes
      any incoming messages (including other requests) while waiting.

Message protocol:
    Requests:    { 'id': <unique>, 'msg': 'call',
                   'target': <int or None>, 'func': <str>, 'args': <tuple> }

                 { 'id': <unique>, 'msg': 'registerCallback',
                   'func': <str> }

                 { 'msg': 'noop' }

    Responses:   { 'id': <same>, 'msg': 'result', 'error': <bool>,
                   'result': <any> }

The `id` is a unique identifier (e.g.: incremental counter) that
allows the receiver to match a response to the original request.

Socket setup:
    * Server (self.server = True)  -> binds a DEALER socket.
    * Client (self.server = False) -> connects a DEALER socket.
    Only a single client is supported (the DEALER socket is connected to one peer).

Function lookup:
    Both sides maintain a table `self.callables` that maps function names to
    Python callables. When a 'call' request arrives, the function is looked up
    in this table and executed. A 'registerCallback' request installs a wrapper
    that, when called, will send a remote 'call' to the other side.
"""

import zmq
import cbor2
import numpy as np
from typing import Any, Callable, Dict, Optional, Tuple

import sim


class ZMQRemoteAPI:
    def __init__(self, *, host: str = '127.0.0.1', port: int = 24020, name: str | None = None, server: bool = False, verbose: int = 0, **kwargs):
        self.name = name
        self.server = server
        self.verbose = verbose
        self.lastSendTime = 0
        self.lastRecvTime = 0
        self.keepAliveInterval = 5
        import time
        self.time = time.time
        self.print = print

        self._context = zmq.Context()
        # Both sides use DEALER to allow asynchronous bidirectional communication.
        self._socket = self._context.socket(zmq.DEALER)

        if self.server:
            self._socket.bind(f'tcp://*:{port}')
        else:
            self._socket.connect(f'tcp://{host}:{port}')

        # Store local callables (both client and server).
        self.callables: Dict[str, Callable] = {}

        # Pending requests: id -> {'done': bool, 'result': Any, 'error': bool}
        self._pending: Dict[int, Dict[str, Any]] = {}
        self.__nextId = 0

    def cleanup(self) -> None:
        """Close the socket and terminate the ZMQ context."""
        if self._socket:
            self._socket.close()
            self._context.term()
            self._socket = None

    def __del__(self) -> None:
        self.cleanup()

    def log(self, level: int, *args: Any) -> None:
        if level <= self.verbose:
            msg = 'ZMQRemoteAPI'
            if self.name:
                msg += f'[{self.name}]'
            for arg in args:
                msg += f' {arg!s}'
            self.print(msg)

    # XXX: This method must be overridden if object‑oriented calls with `target` are needed.
    def callMethod(self, target: int, method_name: str, *args: Any) -> Any:
        raise NotImplementedError('callMethod must be overridden for target-based calls')

    def _nextId(self) -> int:
        """Generate a unique request ID (simple incremental counter)."""
        self.__nextId += 1
        return self.__nextId

    def _sendRequestAndWait(self, req: Dict[str, Any]) -> Any:
        """
        Helper: sends a request and waits for its response.
        """
        req_id = self._nextId()
        req['id'] = req_id
        self._pending[req_id] = {'done': False, 'result': None, 'error': False}
        self.send(req)
        self.spinUntilComplete(req_id)
        pending = self._pending.get(req_id)
        assert pending and pending['done']
        del self._pending[req_id]
        if pending['error']:
            raise Exception(pending['result'])
        else:
            result = pending['result']
            # Unpack the result:
            if result is None:
                return None
            # Convert to tuple if it's a list or tuple; otherwise keep as-is.
            if isinstance(result, (list, tuple)):
                if len(result) == 1:
                    return result[0]
                return tuple(result)
            return result

    def _sendKeepAlive(self):
        """
        Helper: sends a keep-alive message (noop) if needed.
        """
        if self.keepAliveInterval <= 0: return
        if self.lastSendTime + self.keepAliveInterval < self.time():
            self.send({'msg': 'noop'})

    def isRemoteAlive(self):
        """
        Helper: return True if remote is alive (i.e. has sent any message within
        double of the keepAliveInterval).
        """
        if self.keepAliveInterval <= 0: return True
        if self.lastRecvTime <= 0: return True
        return self.lastRecvTime + 2 * self.keepAliveInterval >= self.time()

    def call(self, target: Optional[int], func_name: str, *args: Any) -> Any:
        """
        Remote procedure call. Sends a 'call' request and waits for the response.
        """
        assert isinstance(func_name, str), 'func_name must be a string'
        req = {'msg': 'call', 'target': target, 'func': func_name, 'args': args}
        return self._sendRequestAndWait(req)

    def registerCallback(self, func_name: str, func: Callable, sysCall: bool = False) -> None:
        """
        Registers a local function as a callback on the remote side.
        The remote side will store a wrapper that, when called, will invoke this
        local function via a remote 'call' request.
        This method sends a 'registerCallback' request and waits for acknowledgment.
        """
        assert isinstance(func_name, str), 'func_name must be a string'
        assert callable(func), 'callback must be a callable'
        self.callables[func_name] = func

        if not self.server:
            req = {'msg': 'registerCallback', 'func': func_name, 'sysCall': sysCall}
            self._sendRequestAndWait(req)

    def handleRequest(self, req: Dict[str, Any]) -> None:
        """
        Process a single incoming request (call or registerCallback).
        This is the core dispatcher on both client and server.

        For 'call':
            - Looks up the function in self.callables.
            - If `target` is given, uses self.callMethod (must be overridden).
            - Executes the function, catches errors, and sends a 'result' response
              with the same `id` as the request.

        For 'registerCallback':
            - Installs a wrapper in self.callables that, when called, will send a
              remote 'call' to the other side (effectively invoking the remote function).
            - Replies with a 'result' to confirm.

        Note: The request is expected to have an `id` field, which is used in the response.
        """
        msg = req.get('msg')
        if not isinstance(msg, str):
            self.log(1, 'malformed request: missing msg')
            return

        req_id = req.get('id', -1)

        if msg == 'call':
            func_name: str = req['func']
            args = req.get('args', ())
            try:
                if req.get('target') is not None:
                    # Object‑oriented call – must be implemented by a subclass.
                    result = self.callMethod(req['target'], func_name, *args)
                else:
                    func = self.callables.get(func_name)
                    if func is None:
                        raise NameError(f'No such function: {func_name}')
                    if not callable(func):
                        raise TypeError(f'Not a callable: {func_name}')
                    result = func(*args)

                # Normalize result: wrap single non‑tuple values, keep tuples as is.
                if result is None:
                    result = ()
                elif not isinstance(result, tuple):
                    result = (result,)
                error = False
            except Exception as e:
                result = str(e)
                error = True

            self.send({'msg': 'result', 'id': req_id, 'error': error, 'result': result})
        elif msg == 'registerCallback':
            func_name: str = req['func']
            try:
                # Store a wrapper that calls back to the remote side.
                self.callables[func_name] = lambda *args: self.call(None, func_name, *args)
                if req['sysCall']:
                    #globals()[func_name] = self.callables[func_name]
                    raise Exception('not supported')
                error = False
                result = True
            except Exception as e:
                error = True
                result = str(e)

            self.send({'msg': 'result', 'id': req_id, 'error': error, 'result': result})
        elif msg == 'result':
            pend = self._pending.get(req_id)
            if pend:
                pend['result'] = req['result']
                pend['error'] = req.get('error', False)
                pend['done'] = True
            else:
                self.log(1, f'received result for unknown id: {req_id}')
        elif msg == 'noop':
            pass
        else:
            self.log(1, f'unsupported message: {msg}')

    def spinOnce(self):
        """
        Processes a single incoming message if one is available.
        Returns true if a message was received and handled, otherwise False.
        """
        self._sendKeepAlive()
        msg = self.recv(block=False)
        if msg:
            self.handleRequest(msg)
            return True
        else:
            return False

    def spinSome(self, timeout: float | None = None):
        """
        Processes incoming messages for up to 'timeout' seconds (default: indefinitely).
        Continues processing as long as messages are available, but stops early if no message
        arrives within a short poll interval (max 0.1s).
        """
        if timeout:
            start = self.time()
            while True:
                remaining = timeout - (self.time() - start)
                if remaining <= 0: return
                self._sendKeepAlive()
                if self.poll(min(remaining, 0.1)):
                    self.spinOnce()
        else:
            while self.spinOnce(): pass

    def spinUntilComplete(self, req_id):
        """
        Blocks and processes incoming messages until the request with the given 'req_id'
        is marked as complete in the pending requests table.
        """
        assert(req_id)
        while True:
            pending = self._pending.get(req_id)
            if pending and pending['done']:
                return
            self.spinOnce()

    def poll(self, timeout: float = 0) -> bool:
        """
        Low-level poll: poll for one incoming message with a timeout (in seconds).
        Returns True if a message is available, False otherwise.
        """
        if not self._socket:
            raise RuntimeError('Socket not available')

        return self._socket.poll(int(timeout * 1000), zmq.POLLIN) > 0

    def send(self, msg: Dict[str, Any], block: bool = False) -> None:
        """Low-level sender: CBOR-encodes the dict and sends it."""
        if not self._socket:
            raise RuntimeError('Socket not available')
        self.log(2, 'sending:', msg)
        data = cbor2.dumps(msg, encoders=self._encoders(), default=self._encode_default)
        self._socket.send(data, flags=0 if block else zmq.DONTWAIT)
        self.lastSendTime = self.time()
        self.log(2, 'sent')

    def recv(self, block: bool = True) -> Optional[Dict[str, Any]]:
        """
        Low-level receiver: receives and CBOR-decodes a message.

        @param block: if True, blocks indefinitely; if False, uses NOBLOCK.
        Returns the decoded dict, or None if no message was available (only when block=False).
        """
        if not self._socket:
            raise RuntimeError('Socket not available')
        if block:
            self.log(2, 'receiving... (block)')
            data = self._socket.recv()
        else:
            try:
                data = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                return None
        self.log(2, f'received {len(data)} bytes')
        try:
            msg = cbor2.loads(data, tag_hook=self._tag_hook)
        except Exception as e:
            self.log(1, f'invalid CBOR data: {e!s}')
            self.log(3, f'CBOR payload: {data.hex(' ')}')
            return None
        self.log(2, 'received:', msg)
        self.lastRecvTime = self.time()
        return msg

    def _tag_hook(self, tag: cbor2.CBORTag, immutable: bool) -> Any:
        """
        CBOR tag hook for decoding special types (numpy arrays, sim objects, etc.)
        This is identical to the original ZMQRemoteAPI.
        """

        if tag.tag == Matrix.CBOR_TAG:
            return Matrix.fromcbor(tag)
        if tag.tag in range(64, 88):
            _TAG_TO_DTYPE = {
                64: np.dtype("u1"),        # U8
                65: np.dtype(">u2"),       # U16BE
                66: np.dtype(">u4"),       # U32BE
                67: np.dtype(">u8"),       # U64BE
                68: np.dtype("u1"),        # U8C (same as U8, often char buffer)
                69: np.dtype("<u2"),       # U16LE
                70: np.dtype("<u4"),       # U32LE
                71: np.dtype("<u8"),       # U64LE
                72: np.dtype("i1"),        # S8
                73: np.dtype(">i2"),       # S16BE
                74: np.dtype(">i4"),       # S32BE
                75: np.dtype(">i8"),       # S64BE
                77: np.dtype("<i2"),       # S16LE
                78: np.dtype("<i4"),       # S32LE
                79: np.dtype("<i8"),       # S64LE
                80: np.dtype(">f2"),       # F16BE
                81: np.dtype(">f4"),       # F32BE
                82: np.dtype(">f8"),       # F64BE
                83: np.dtype(">f16"),      # F128BE (may not be supported everywhere)
                84: np.dtype("<f2"),       # F16LE
                85: np.dtype("<f4"),       # F32LE
                86: np.dtype("<f8"),       # F64LE
                87: np.dtype("<f16"),      # F128LE (platform dependent)
            }
            if tag.tag in _TAG_TO_DTYPE:
                dtype = _TAG_TO_DTYPE[tag.tag]
                payload = tag.value
                if isinstance(payload, memoryview):
                    payload = payload.tobytes()
                elif isinstance(payload, list):
                    # rare fallback: list of ints
                    payload = bytes(payload)
                return np.frombuffer(payload, dtype=dtype)
        if tag.tag == sim.Object.CBOR_TAG:
            return sim.Object.fromcbor(tag)
        if tag.tag == sim.ObjectArray.CBOR_TAG:
            return sim.ObjectArray.fromcbor(tag)
        if tag.tag == sim.Color.CBOR_TAG:
            return sim.Color.fromcbor(tag)
        if tag.tag == sim.Quaternion.CBOR_TAG:
            return sim.Quaternion.fromcbor(tag)
        if tag.tag == sim.Pose.CBOR_TAG:
            return sim.Pose.fromcbor(tag)
        if tag.tag == 4294999997:
            # zmq remote callback
            return lambda *args: self.call(None, tag.value, *args)
        return tag

    def _encoders(self):
        return {
            sim.Object: lambda e, v: e.encode(v.__cbor__()),
            sim.ObjectArray: lambda e, v: e.encode(v.__cbor__()),
            sim.Color: lambda e, v: e.encode(v.__cbor__()),
            sim.Matrix: lambda e, v: e.encode(v.__cbor__()),
            sim.Quaternion: lambda e, v: e.encode(v.__cbor__()),
            sim.Pose: lambda e, v: e.encode(v.__cbor__()),
        }

    def _encode_default(self, encoder: cbor2.CBOREncoder, value: Any) -> None:
        if callable(value):
            name = f'@tmpcallback_function_{id(value)}'

            if name not in self.callables:
                self.callables[name] = value

            ## If we are inside _sendRequestAndWait, remember this temporary callback
            #if getattr(self, '_callback_scope', None) is not None:
            #    self._callback_scope.append(name)

            encoder.encode(cbor2.CBORTag(4294999997, name))
        else:
            raise cbor2.CBOREncodeTypeError(f'cannot serialize type {type(value)}')


class PythonWrapper:
    def __init__(self, *, port: int, client_id: str, sysCalls: list[str], verbose: int = 0, barePrint: bool = False, **kwargs):
        self.rapi = ZMQRemoteAPI(
            name = f'pythonWrapper-pyClient-{client_id}',
            server = False,
            port = port,
            verbose = verbose,
        )
        if barePrint is False:
            self.rapi.print = lambda *args: self.rapi.call(None, '__print', *map(str, args))

        self.sysCalls = sysCalls
        sim.Object._callMethod = self.rapi.call
        self.rapi.log(1, 'initializing...')

    def registerSysCalls(self, globalsDict: dict[str, Any]):
        self.rapi.log(1, 'registering sysCalls...')
        for name in self.sysCalls:
            if callable(func := globalsDict.get(name)):
                self.rapi.log(2, f'registering sysCall {name}...')
                self.rapi.registerCallback(name, func, False)

    def spin(self):
        self.rapi.log(1, 'ready')
        self.rapi.call(None, '__ready')
        self.rapi.log(1, 'spinning')
        while 1:
            self.rapi.spinSome()
