"""
ZMQRemoteAPI – dual-role client/server over a single REQ/REP socket.

Architecture overview:
----------------------
The class can act either as a client (REQ, connects) or as a server (REP, binds).
Messages are encoded with CBOR and must follow the strict REQ/REP alternation.

Normal RPC call (client → server):
    1. client sends {'msg':'call', 'func':'add', 'args':(1,2)}
    2. server executes add(1,2), replies {'msg':'result', 'result':(3,)}
    3. client receives the result and returns it.

Client‑registered callback (server → client):
    1. client sends {'msg':'registerCallback', 'func':'progress'}
    2. server stores a wrapper in self.callables['progress'] that, when called,
       will perform a remote call back to the client.
    3. Later, during a normal call, the server calls progress(50).
    4. The wrapper invokes self.call(None, 'progress', 50).
       Because the server is still inside the original handle_request and hasn't
       sent its final reply, this send becomes the *reply* to the client's current
       request.
    5. Client receives {'msg':'call', 'func':'progress', 'args':(50,)}, executes
       its local function, and sends {'msg':'result', 'result':...} as its *next request*.
    6. Server receives that result, continues executing the original function,
       and finally sends the ultimate 'result' reply to the client.

This design keeps the REQ/REP alternation intact: every send is always a reply
to the previous recv, and vice versa. The nested self.call on the server is
possible because it occupies the “reply” slot of the current transaction.

Note on function lookup:
    Unlike the Lua version (which uses _G for all functions), this Python
    implementation uses self.callables exclusively. The server can only call
    functions that have been registered via register_callback. This is intentional
    for explicit registration and security.
"""


import uuid
import zmq
import cbor2
import numpy as np
from typing import Any, Callable

import sim


class ZMQRemoteAPI:
    def __init__(self, opts: dict[str, Any] | None = None):
        opts = opts or {}
        self.name = opts.get('name')
        self.server = bool(opts.get('server', False))
        self.verbose = opts.get('verbose', 0)
        self.client_id = opts.get('clientID', str(uuid.uuid4()))

        self._context = zmq.Context()
        socket_type = zmq.REP if self.server else zmq.REQ
        self._socket = self._context.socket(socket_type)

        host = opts.get('host', '127.0.0.1')
        port = opts.get('port', 24020)
        if self.server:
            self._socket.bind(f'tcp://*:{port}')
        else:
            self._socket.connect(f'tcp://{host}:{port}')

        # Client: stores user-provided callbacks (to be invoked when a remote
        #        'call' for that name arrives).
        # Server: stores wrappers that, when called, trigger a remote callback
        #        to the client.
        self.callables: dict[str, Callable] = {}

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

    def call(self, target: int | None, func_name: str, *args: Any) -> Any | tuple[Any, ...] | None:
        """
        Client-side entry point for a remote procedure call.

        Flow:
            1. Sends a 'call' message to the server.
            2. Enters a loop that receives replies.
               - If the reply is a 'result' -> unpack and return it.
               - Otherwise (e.g., a 'call' from the server, which is a callback
                 invocation) -> handle it locally via handle_request and send
                 back a 'result', then continue waiting for the final reply.

        This method is also used by the server's callback wrapper to invoke a
        remote function on the client. In that context, the send acts as the
        reply to the client's current request, and the subsequent recv waits for
        the client's answer – all while keeping the REQ/REP alternation valid.
        """
        self.send({'msg': 'call', 'target': target, 'func': func_name, 'args': args})
        while True:
            rep = self.recv(block=True)
            if rep is None:
                print('WARNING: rep is None')
                continue  # Should not happen with blocking recv

            if rep['msg'] == 'result':
                if rep.get('error', False):
                    raise Exception(rep['result'])
                ret = tuple(rep['result'])
                if len(ret) == 1: return ret[0]
                if len(ret) > 1: return ret
                return
            else:
                # The other side is asking us to execute a callback (or any other
                # request). Handle it locally and reply, then loop again.
                self.handle_request(rep)

    def register_callback(self, func_name: str, func: Callable) -> None:
        """
        Client-side setup: registers a local function as a callback on the server.

        It sends a 'registerCallback' message to the server. The server stores
        a wrapper that, when called, will invoke this client's function remotely.
        This client also stores the local function in self.callables so that
        when a remote 'call' for this name arrives, it can be executed.
        """
        self.send({'msg': 'registerCallback', 'func': func_name})
        rep = self.recv(block=True)
        if rep is None:
            raise RuntimeError('No reply from server')
        if rep.get('msg') != 'result':
            raise RuntimeError('Invalid server reply')
        if rep.get('error', False):
            raise RuntimeError(f'registerCallback failed: {rep["result"]}')
        self.callables[func_name] = func

    def handle_request(self, req: dict[str, Any]) -> None:
        """
        Process a single incoming request (either a 'call' or a 'registerCallback').

        This is the core of both the server and the client request processing.

        'call' branch (server side or client side when receiving a callback):
            - Looks up the function in self.callables (in the server context,
              this is the wrapper that triggers a remote callback; in the client
              context, it is the local user-defined function).
            - Calls it with the provided arguments.
            - Important: the called function may itself invoke self.call(...)
              (e.g., a server-side wrapper). Those nested calls will *send a message*
              (which is the reply to the current pending request) and *receive the
              result* (the next request from the other side). This nested loop is
              perfectly legal because each send/recv pair completes a full REQ/REP
              transaction.
            - After the function returns, this method sends the final 'result' reply.

        'registerCallback' branch (server only):
            - Stores a wrapper in self.callables that calls back to the client.
            - Replies with a 'result' to confirm registration.

        Note: The server can only call functions that have been explicitly registered
              via register_callback (unlike the Lua version which uses _G).
        """
        msg = req.get('msg')
        if not isinstance(msg, str):
            self.log(1, 'malformed request: missing msg')
            return

        if msg == 'call':
            func_name: str = req['func']
            args = req.get('args', ())
            try:
                func = self.callables.get(func_name)
                if func is None:
                    raise NameError(f'No such function: {func_name}')
                if not callable(func):
                    raise TypeError(f'Not a callable: {func_name}')
                result = func(*args)
                if result is None:
                    result = ()
                elif not isinstance(result, tuple):
                    result = (result,)
                error = False
            except Exception as e:
                result = str(e)
                error = True
            self.send({'msg': 'result', 'error': error, 'result': result})

        elif msg == 'registerCallback':
            func_name: str = req['func']
            try:
                # Server stores a wrapper that, when called, will invoke the
                # remote function on the client via self.call.
                self.callables[func_name] = lambda *args: self.call(None, func_name, *args)
                error = False
                result = None
            except Exception as e:
                error = True
                result = str(e)
            self.send({'msg': 'result', 'error': error, 'result': result})

        else:
            self.log(1, 'unsupported message:', msg)

    def handle_requests(self) -> None:
        """
        Server-side main loop: repeatedly receives and processes pending requests.
        This should be called only when self.server is True.

        The loop uses non-blocking recv, so it will process all messages currently
        in the socket buffer and then exit. (In the Lua version, it also breaks
        when no message is available.)
        """
        if not self.server:
            raise RuntimeError('handle_requests should be called only from server')
        while True:
            req = self.recv(block=False)
            if req is None:
                break
            self.handle_request(req)

    def send(self, msg: dict[str, Any]) -> None:
        """Low-level sender: CBOR-encodes the dict and sends it."""
        if not self._socket:
            raise RuntimeError('Socket not available')
        self.log(2, 'sending:', msg)
        data = cbor2.dumps(msg)
        self._socket.send(data)
        self.log(2, 'sent')

    def recv(self, block: bool = True) -> dict[str, Any] | None:
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
            req = cbor2.loads(data, tag_hook=self._tag_hook)
        except Exception as e:
            self.log(1, 'invalid request CBOR data:', e)
            return None
        self.log(2, 'received:', req)
        return req

    # The tag_hook is used for decoding special CBOR tags (numpy arrays, sim objects, etc.)
    # It is not part of the core communication logic.
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
            raise NotImplemented
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
