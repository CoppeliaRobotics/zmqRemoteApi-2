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
    * A `handleRequests()` loop is provided to process incoming messages
      indefinitely (useful for servers or clients that need to respond to callbacks).

Message protocol:
    Requests:    { 'id': <unique>, 'msg': 'call',
                   'target': <int or None>, 'func': <str>, 'args': <tuple> }

                 { 'id': <unique>, 'msg': 'registerCallback',
                   'func': <str> }

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
from time import time
from typing import Any, Callable, Dict, Optional, Tuple

import sim


class ZMQRemoteAPI:
    def __init__(self, opts: Optional[Dict[str, Any]] = None):
        opts = opts or {}
        self.name = opts.get('name')
        self.server = bool(opts.get('server', False))
        self.verbose = opts.get('verbose', 0)
        self.lastSendTime = time()
        self.lastRecvTime = time()

        self._context = zmq.Context()
        # Both sides use DEALER to allow asynchronous bidirectional communication.
        self._socket = self._context.socket(zmq.DEALER)

        host = opts.get('host', '127.0.0.1')
        port = opts.get('port', 24020)
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
            tag = 'ZMQRemoteAPI'
            if self.name:
                tag += f'[{self.name}]'
            print(tag, *args)

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

        while True:
            pending = self._pending.get(req_id)
            if pending and pending['done']:
                result = pending['result']
                error = pending['error']
                del self._pending[req_id]
                if error:
                    raise Exception(result)
                # Unpack the result:
                if result is None:
                    return None
                # Convert to tuple if it's a list or tuple; otherwise keep as-is.
                if isinstance(result, (list, tuple)):
                    if len(result) == 1:
                        return result[0]
                    return tuple(result)
                return result

            msg = self.recv(block=True)
            if msg:
                self.handleRequest(msg)

    def call(self, target: Optional[int], func_name: str, *args: Any) -> Any:
        """
        Remote procedure call. Sends a 'call' request and waits for the response.
        """
        assert isinstance(func_name, str), 'func_name must be a string'
        req = {'msg': 'call', 'target': target, 'func': func_name, 'args': args}
        return self._sendRequestAndWait(req)

    def registerCallback(self, func_name: str, func: Callable, global_: bool = False) -> None:
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
            req = {'msg': 'registerCallback', 'func': func_name, 'global': global_}
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

        req_id = req.get('id')
        if req_id is None:
            self.log(1, 'request missing id')
            return

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
                if req['global']:
                    globals()[func_name] = self.callables[func_name]
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
        else:
            self.log(1, f'unsupported message: {msg}')

    def handleRequests(self, timeout: float = -1) -> None:
        """
        Main loop for processing incoming requests.
        If timeout is negative, the loop runs indefinitely until an error occurs or the socket is closed.
        If timeout is positive, it runs for at most the specified amount of seconds.
        """
        def processOneMessage():
            msg = self.recv(block=True)
            if msg:
                self.handleRequest(msg)
                return True

        if timeout < 0:
            while processOneMessage():
                pass
        else:
            start = time()
            while True:
                remaining = timeout - (time() - start)
                if remaining <= 0:
                    break
                if self.poll(min(remaining, 0.1)):
                    processOneMessage()


    def poll(self, timeout: float = 0) -> bool:
        """
        Low-level poll: poll for one incoming message with a timeout (in seconds).
        Returns True if a message is available, False otherwise.
        """
        if not self._socket:
            raise RuntimeError('Socket not available')

        return self._socket.poll(int(timeout * 1000), zmq.POLLIN) > 0

    def send(self, msg: Dict[str, Any]) -> None:
        """Low-level sender: CBOR-encodes the dict and sends it."""
        if not self._socket:
            raise RuntimeError('Socket not available')
        self.log(2, 'sending:', msg)
        data = cbor2.dumps(msg)
        self._socket.send(data)
        self.lastSendTime = time()
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
            self.log(2, 'receiving... (non‑block)')
            try:
                data = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                return None
        try:
            msg = cbor2.loads(data, tag_hook=self._tag_hook)
        except Exception as e:
            self.log(1, 'invalid CBOR data:', e)
            return None
        self.log(2, 'received:', msg)
        self.lastRecvTime = time()
        return msg

    # ----------------------------------------------------------------------
    # CBOR tag hook for decoding special types (numpy arrays, sim objects, etc.)
    # This is identical to the original ZMQRemoteAPI.
    # ----------------------------------------------------------------------
    def _tag_hook(self, decoder: cbor2.CBORDecoder, tag: cbor2.CBORTag) -> Any:
        if tag.tag == 40:
            # ND-array
            dims, data = tag.value
            arr = np.array(data, dtype=np.float64)
            return arr.reshape(dims)
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
        if tag.tag == 4294999999:
            handle = tag.value
            return sim.Object(handle)
        if tag.tag == 4294999998:
            # handlearray -> ObjectArray
            raise NotImplementedError("handlearray decoding not implemented")
        if tag.tag == 4294970000:
            # color
            return tuple(tag.value)
        if tag.tag == 4294980000:
            # quaternion
            return np.array(tag.value, dtype=np.float64)
        if tag.tag == 4294980500:
            # pose
            return np.array(tag.value, dtype=np.float64)
        return tag
