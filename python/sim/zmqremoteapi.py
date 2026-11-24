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
                   'result': <list> }

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

import sys
import zmq
import cbor2
import numpy as np
from typing import Any, Callable, Dict, Optional, Tuple
from pathlib import Path

import sim


class RemoteGoneError(RuntimeError):
    """Raised when the remote side is considered gone (no reply and no keep-alive)."""


# RFC8746 typed-array tags -> numpy dtype. Kept at module level so that decoding
# arrays (images, buffers, matrices) does not rebuild this table for every value.
_TAG_TO_DTYPE = {
    64: np.dtype("u1"),        # U8
    65: np.dtype(">u2"),       # U16BE
    66: np.dtype(">u4"),       # U32BE
    67: np.dtype(">u8"),       # U64BE
    68: np.dtype("u1"),        # U8C (same as U8, often char buffer)
    69: np.dtype("<u2"),        # U16LE
    70: np.dtype("<u4"),        # U32LE
    71: np.dtype("<u8"),        # U64LE
    72: np.dtype("i1"),        # S8
    73: np.dtype(">i2"),        # S16BE
    74: np.dtype(">i4"),        # S32BE
    75: np.dtype(">i8"),        # S64BE
    77: np.dtype("<i2"),        # S16LE
    78: np.dtype("<i4"),        # S32LE
    79: np.dtype("<i8"),        # S64LE
    80: np.dtype(">f2"),        # F16BE
    81: np.dtype(">f4"),        # F32BE
    82: np.dtype(">f8"),        # F64BE
    84: np.dtype("<f2"),        # F16LE
    85: np.dtype("<f4"),        # F32LE
    86: np.dtype("<f8"),        # F64LE
    # 83 (F128BE) and 87 (F128LE) are deliberately omitted: numpy has no
    # portable 128-bit float dtype.
}


class _Table(dict):
    """A Lua table received by value: supports t.key as well as t['key']."""

    def __getattr__(self, k: str) -> Any:
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k) from None


def _toValue(v: Any) -> Any:
    if isinstance(v, dict):
        return _Table({k: _toValue(x) for k, x in v.items()})
    if isinstance(v, list):
        return [_toValue(x) for x in v]
    return v


def _encodeArg(v: Any) -> Any:
    """Converts a call argument: a LuaObjectProxy becomes its registry marker."""
    if isinstance(v, LuaObjectProxy):
        return {'__luaObject': v._id}
    if isinstance(v, dict):
        return {k: _encodeArg(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_encodeArg(x) for x in v]
    return v


def _encodeArgs(args: tuple) -> list:
    return [_encodeArg(a) for a in args]


def _convertResult(v: Any, client: 'ZMQRemoteAPI', module_name: str) -> Any:
    """Converts a module result: Lua object markers become LuaObjectProxy."""
    if isinstance(v, dict):
        if '__luaObject' in v:
            return LuaObjectProxy(client, module_name, v['__luaObject'])
        return _Table({k: _convertResult(x, client, module_name) for k, x in v.items()})
    if isinstance(v, list):
        return [_convertResult(x, client, module_name) for x in v]
    return v


def _unpackResult(res: Any) -> Any:
    if isinstance(res, (list, tuple)):
        if len(res) == 0: return None
        if len(res) == 1: return res[0]
        return tuple(res)
    return res


class LuaObjectProxy:
    """
    Proxy to a Lua object created by a module, e.g. the instance returned by
    `Motion.MoveToPose(params)`.

    The Lua object lives in the Lua state; it is kept in a registry there and
    addressed by an integer id. Attribute access yields a callable that invokes
    the method **with the object as self**, so the Lua side sees a normal
    `object:method(...)` call:

        motion = Motion.MoveToPose(params)
        motion.run()
        motion.remove()
    """

    def __init__(self, client: 'ZMQRemoteAPI', module_name: str, object_id: int):
        self._client = client
        self._module = module_name
        self._id = object_id
        self._members: Dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        if name.startswith('_'):
            raise AttributeError(name)
        try:
            return self._members[name]
        except KeyError:
            pass
        info = self._client.call(
            None, '__moduleObjectMember', self._module, self._id, name) or {}
        kind = info.get('kind')
        if kind == 'function':
            def member(*args: Any) -> Any:
                return self._invoke(name, args)
            member.__name__ = f'{self._module}.{name}'
        elif kind == 'value':
            member = _convertResult(info.get('value'), self._client, self._module)
        elif kind == 'object':
            member = LuaObjectProxy(self._client, self._module, info.get('id'))
        elif kind == 'nil':
            raise AttributeError(f"module object has no attribute '{name}'")
        else:
            member = _Table()
        self._members[name] = member
        return member

    def _invoke(self, name: str, args: tuple) -> Any:
        try:
            res = self._client.call(
                None, '__moduleObjectCall', self._module, self._id, name, _encodeArgs(args))
        except Exception as e:
            raise RuntimeError(f'{self._module}.{name}: {e}') from e
        if isinstance(res, (list, tuple)):
            return _unpackResult([_convertResult(r, self._client, self._module) for r in res])
        return _convertResult(res, self._client, self._module)

    def __repr__(self) -> str:
        return f'sim.LuaObject({self._module!r}, {self._id})'


class ModuleProxy:
    """
    Proxy to one of CoppeliaSim's Lua modules (e.g. 'simUI', 'simOMPL',
    'simIK', 'simVision', 'simAssimp', 'Motion').

    Those modules are Lua tables (often backed by a C plugin, e.g.
    `simUI = loadPlugin 'simUI'`) living in the Lua state of the script or
    worker; they cannot be imported into the Python process. The proxy runs
    them on that Lua state:

        simUI = rapi.module('simUI')
        ui = simUI.create(xml)
        simUI.show(ui)
        simUI.curve_type.xy          # constants/sub-tables work too
        simUI.curve_style.scatter

        Motion = rapi.module('Motion')   # Lua objects (classes) work too
        motion = Motion.MoveToPose(params)
        motion.run()

    Each attribute is resolved on first access (and cached) as a function or
    callable table, a value, a sub-table, or a Lua object: functions become
    callables, plain tables become dicts that also allow attribute access
    (`_Table`), sub-tables containing functions become nested proxies, and Lua
    objects become `LuaObjectProxy` instances.

    Function return values follow Lua's multiple returns: one value as-is,
    several as a tuple, none as None. Only CBOR-representable values can be
    transferred (numbers, strings, booleans, lists, maps); Lua objects are
    represented by a registry id and wrapped in a proxy.
    """

    def __init__(self, client: 'ZMQRemoteAPI', name: str, path: tuple = ()):
        self._client = client
        self._name = name
        self._path = tuple(path)
        self._members: Dict[str, Any] = {}

    def __getattr__(self, k: str) -> Any:
        if k.startswith('_'):
            raise AttributeError(k)
        try:
            return self._members[k]
        except KeyError:
            pass
        path = self._path + (k,)
        info = self._client.call(None, '__moduleMember', self._name, list(path)) or {}
        kind = info.get('kind')
        if kind == 'function':
            def member(*args: Any) -> Any:
                return self._invoke(path, args)
            member.__name__ = '.'.join((self._name,) + path)
        elif kind == 'value':
            member = _convertResult(info.get('value'), self._client, self._name)
        elif kind == 'object':
            member = LuaObjectProxy(self._client, self._name, info.get('id'))
        elif kind == 'nil':
            raise AttributeError(f"module '{self._name}' has no attribute '{k}'")
        else:
            member = ModuleProxy(self._client, self._name, path)
        self._members[k] = member
        return member

    def _invoke(self, path: tuple, args: tuple) -> Any:
        try:
            res = self._client.call(None, '__moduleCall', self._name, list(path), _encodeArgs(args))
        except Exception as e:
            raise RuntimeError(
                f"{self._name}.{'.'.join(map(str, path))}: {e}") from e
        if isinstance(res, (list, tuple)):
            return _unpackResult([
                _convertResult(r, self._client, self._name) for r in res])
        return _convertResult(res, self._client, self._name)

    def __repr__(self) -> str:
        p = '.'.join(self._path)
        return f'sim.Module({self._name!r}' + (f', {p!r})' if p else ')')


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
        # Never block on close/term because of messages that can no longer be
        # delivered (e.g. when the peer has disappeared).
        self._socket.setsockopt(zmq.LINGER, 0)

        if self.server:
            self._socket.bind(f'tcp://*:{port}')
        else:
            self._socket.connect(f'tcp://{host}:{port}')

        # Store local callables (both client and server).
        self.callables: Dict[str, Callable] = {}

        # Pending requests: id -> {'done': bool, 'result': list[Any], 'error': bool}
        self._pending: Dict[int, Dict[str, Any]] = {}
        self.__nextId = 0

        # Names of the calls currently being handled, innermost last. Used to
        # mark re-entrant requests as 'nested', and to verify that step() is
        # really executed in the thread callback.
        self._callStack: list[str] = []

        # The encoder map is constant: build it once instead of for each message.
        self._encoders_cache = self._encoders()

        # Set when the remote side sends 'bye' (it is about to disappear).
        self._remoteBye = False

        # Optional owner hook: resolves an incoming call name that is not in
        # self.callables (e.g. the embedded wrapper resolves it from the user
        # script's namespace, so 'myFunc#ext' can reach a plain Python function).
        self.resolveCallable: Optional[Callable[[str], Any]] = None

        # Silence (in seconds) after which the remote side is considered gone.
        # CoppeliaSim can halt *all* scripts for a while (popup menu, modal
        # dialog, edit mode) without being dead, so this is generous by default;
        # 0 disables the check entirely (the embedded wrapper does that: the Lua
        # side kills this process when the script is cleaned up).
        self.remoteSilenceTimeout: float | None = 60.0

        # Set once sysCall_cleanup has run (called by the remote side, or by
        # cleanup() below when we are the first to leave).
        self._sysCallCleanupDone = False

    def cleanup(self) -> None:
        """Run sysCall_cleanup if needed, tell the remote side we are leaving, then close."""
        if self._socket:
            # If the remote side did not run our sysCall_cleanup (typically
            # because we are the first to leave), run it now, while the
            # connection is still up. The worker runs it itself when it
            # terminates (e.g. at simulation end), in which case this is skipped.
            if not self._sysCallCleanupDone and 'sysCall_cleanup' in self.callables:
                self._sysCallCleanupDone = True
                try:
                    self.callables['sysCall_cleanup']()
                except Exception as e:
                    self.log(1, 'sysCall_cleanup failed:', e)
            try:
                # ZMQ does not notify the peer when a DEALER socket disappears
                # (killing this process looks exactly like silence). Say goodbye
                # explicitly so that the remote side can terminate right away
                # instead of waiting for its liveness timeout.
                self.send({'msg': 'bye'}, log=False)
                self._socket.setsockopt(zmq.LINGER, 200)  # let the bye flush
            except Exception:
                pass
            self._socket.close()
            self._context.term()
            self._socket = None
            self._context = None

    def __del__(self) -> None:
        try:
            self.cleanup()
        except Exception:
            # During interpreter shutdown the zmq objects may already be gone.
            pass

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

    def _sendRequestAndWait(self, req: Dict[str, Any], *, log: bool = True, timeout: float | None = None) -> Any:
        """
        Helper: sends a request and waits for its response.
        With `timeout`, raises TimeoutError if no reply arrives in time (useful
        e.g. for the initial handshake with the server).
        """
        req_id = self._nextId()
        req['id'] = req_id
        self._pending[req_id] = {'done': False, 'result': None, 'error': False}
        try:
            self.send(req, log=log)
            self.spinUntilComplete(req_id, log=log, timeout=timeout)
            pending = self._pending[req_id]
        finally:
            # Never leak a pending entry, whatever happened (timeout, remote
            # gone, keyboard interrupt, ...).
            self._pending.pop(req_id, None)
        if pending['error']:
            raise Exception(pending['result'])
        else:
            result = pending['result']
            # Unpack the result:
            if result is None:
                raise TypeError('protocol error: field "result" missing')
            # Convert to tuple if it's a list or tuple; otherwise keep as-is.
            if isinstance(result, (list, tuple)):
                if len(result) == 1:
                    return result[0]
                if len(result) > 1:
                    return tuple(result)
                return
            raise TypeError('protocol error: field "result" must be a list')

    def _sendKeepAlive(self):
        """
        Helper: sends a keep-alive message (noop) if needed.

        A failed keep-alive is not fatal: the remote side may be gone already (or
        the socket may be closing); the liveness check in recv() decides what to
        do next.
        """
        if self.keepAliveInterval <= 0: return
        if self.lastSendTime + self.keepAliveInterval < self.time():
            try:
                self.send({'msg': 'noop'})
            except Exception as e:
                self.log(2, 'keep-alive send failed:', e)

    def isRemoteAlive(self):
        """
        Helper: return True if remote is alive (i.e. has sent any message within
        the silence tolerance). A 'bye' message from the remote side makes this
        return False immediately.

        `remoteSilenceTimeout` overrides the tolerance: None = 2 *
        keepAliveInterval, 0 = never (for a peer that cannot notify us but whose
        silence is not death, e.g. CoppeliaSim while a popup menu is open).
        """
        if self._remoteBye:
            return False
        if self.keepAliveInterval <= 0: return True
        if self.lastRecvTime <= 0: return True
        timeout = self.remoteSilenceTimeout
        if timeout is None:
            timeout = 2 * self.keepAliveInterval
        if timeout <= 0:
            return True
        return self.lastRecvTime + timeout >= self.time()

    def call(self, target: Optional[int], func_name: str, *args: Any) -> Any:
        return self.callEx(target, func_name, args)

    def callEx(self, target: Optional[int], func_name: str, args: tuple[Any], *, log: bool = True,
               timeout: float | None = None) -> Any:
        """
        Remote procedure call. Sends a 'call' request and waits for the response.
        With `timeout`, raises TimeoutError if no reply arrives in time (useful
        e.g. for the initial handshake with the server).
        """
        assert isinstance(func_name, str), 'func_name must be a string'
        req = {
            'msg': 'call',
            'target': target,
            'func': func_name,
            'args': args,
            # 'nested' tells the remote side that this call is made while
            # handling a call from the remote side (e.g. a sysCall callback
            # invoked from Lua) and must therefore be executed right away.
            # Calls made from sysCall_thread itself are not 'nested': they
            # originate from the thread body, not from a re-entrant callback.
            'nested': bool(self._callStack) and self._callStack[-1] != 'sysCall_thread',
        }
        return self._sendRequestAndWait(req, log=log, timeout=timeout)

    def runOnlyDuringSimulation(self, enabled: bool = True) -> bool:
        """
        External-client command: ask the worker script to stop serving this
        client once the simulation is stopped (i.e. when
        sim.scene.simulation.state == sim.simulationState.stopped). The worker
        then sends 'bye' and terminates, so pending calls and spin() end
        instead of keeping the client alive outside of a simulation.

        Returns the new state. Pass enabled=False to undo it.
        """
        return self.call(None, 'runOnlyDuringSimulation', enabled)

    def module(self, name: str) -> ModuleProxy:
        """
        Returns a proxy to one of CoppeliaSim's Lua modules (e.g. 'simUI',
        'simOMPL', 'simIK', 'simVision', 'simAssimp'), executed on the remote
        Lua state (the worker script for an external client, the wrapper script
        for an embedded Python script):

            simUI = rapi.module('simUI')
            ui = simUI.create('<ui>...</ui>')
            simUI.show(ui)
        """
        return ModuleProxy(self, name)

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
            func_name = req.get('func')
            if not isinstance(func_name, str):
                self._reply({'msg': 'result', 'id': req_id, 'error': True,
                             'result': "protocol error: missing 'func' field"})
                return
            args = req.get('args', ())
            if req.get('target') is None and func_name == 'sysCall_cleanup':
                self._sysCallCleanupDone = True
            self._callStack.append(func_name)
            try:
                try:
                    if req.get('target') is not None:
                        # Object‑oriented call – must be implemented by a subclass.
                        result = self.callMethod(req['target'], func_name, *args)
                    else:
                        func = self.callables.get(func_name)
                        if func is None and self.resolveCallable is not None:
                            # Owners (e.g. the embedded Python wrapper) may resolve
                            # an unregistered name from the script's own namespace,
                            # so that 'myFunc#ext' reaches a plain Python function.
                            func = self.resolveCallable(func_name)
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
                    import traceback
                    result = ''.join(traceback.format_exception_only(type(e), e)).rstrip()
                    result += '\nTraceback (most recent call last):'
                    def short(s, limit=59):
                        if len(s) > limit:
                            return '...' + s[-56:]
                    for f in traceback.extract_tb(e.__traceback__):
                        result += f'\n  File "{short(f.filename)}", line {f.lineno}, in {f.name}\n    {f.line}'
                    error = True
            finally:
                self._callStack.pop()

            self._reply({'msg': 'result', 'id': req_id, 'error': error, 'result': result})
        elif msg == 'registerCallback':
            func_name = req.get('func')
            try:
                if not isinstance(func_name, str):
                    raise ValueError("protocol error: missing 'func' field")
                # Store a wrapper that calls back to the remote side.
                self.callables[func_name] = lambda *args: self.call(None, func_name, *args)
                if req.get('sysCall', False):
                    #globals()[func_name] = self.callables[func_name]
                    raise Exception('not supported')
                error = False
                result = True
            except Exception as e:
                error = True
                result = str(e)

            self._reply({'msg': 'result', 'id': req_id, 'error': error, 'result': result})
        elif msg == 'result':
            pend = self._pending.get(req_id)
            if pend:
                pend['result'] = req.get('result')
                pend['error'] = req.get('error', False)
                pend['done'] = True
            else:
                self.log(1, f'received result for unknown id: {req_id}')
        elif msg == 'noop':
            pass
        elif msg == 'bye':
            # The remote side is leaving (e.g. a simulation-only worker whose
            # simulation stopped): stop waiting for messages that will never come.
            self._remoteBye = True
        else:
            self.log(1, f'unsupported message: {msg}')

    def spinOnce(self, *, log: bool = True):
        """
        Processes a single incoming message if one is available.
        Returns true if a message was received and handled, otherwise False.
        """
        self._sendKeepAlive()
        msg = self.recv(block=False, log=log)
        if msg:
            self.handleRequest(msg)
            return True
        else:
            return False

    def spinSome(self, timeout: float | None = None, *, log: bool = True):
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
                    self.spinOnce(log=log)
        else:
            while self.spinOnce(log=log): pass

    def spin(self, timeout: float = 0.1) -> None:
        """
        Keep the client alive and serve incoming callbacks, until the remote side
        is considered gone.

        The wait is event-driven: the process sleeps in the socket poll and wakes
        up only for an incoming message (or to send a keep-alive), instead of
        waking up every `timeout` seconds. `timeout` is kept for API compatibility
        (it used to be the polling granularity of the old busy-wait loop).
        """
        while True:
            try:
                msg = self.recv(block=True)
            except RemoteGoneError:
                return
            if msg is not None:
                self.handleRequest(msg)

    def spinUntilComplete(self, req_id, *, log: bool = True, timeout: float | None = None):
        """
        Blocks and processes incoming messages until the request with the given 'req_id'
        is marked as complete in the pending requests table.

        The wait is a true blocking socket receive: it returns as soon as *any*
        message arrives (the reply, but also a re-entrant call from the remote
        side, which is then served immediately). No polling/timeout granularity
        is involved, and the process does not consume CPU while waiting.

        With `timeout`, a TimeoutError is raised when no reply arrives in time.
        """
        assert(req_id)
        if timeout is None:
            while True:
                pending = self._pending.get(req_id)
                if pending and pending['done']:
                    return
                msg = self.recv(block=True, log=log)
                if msg is not None:
                    self.handleRequest(msg)
        else:
            deadline = self.time() + timeout
            while True:
                pending = self._pending.get(req_id)
                if pending and pending['done']:
                    return
                remaining = deadline - self.time()
                if remaining <= 0:
                    self._pending.pop(req_id, None)
                    raise TimeoutError(f'no reply from the remote side within {timeout:g}s')
                if self.poll(remaining):
                    msg = self.recv(block=False, log=log)
                    if msg is not None:
                        self.handleRequest(msg)

    def poll(self, timeout: float = 0) -> bool:
        """
        Low-level poll: poll for one incoming message with a timeout (in seconds).
        Returns True if a message is available, False otherwise.
        """
        if not self._socket:
            raise RuntimeError('Socket not available')

        return self._socket.poll(int(timeout * 1000), zmq.POLLIN) > 0

    def send(self, msg: Dict[str, Any], *, block: bool = False, log: bool = True) -> None:
        """Low-level sender: CBOR-encodes the dict and sends it."""
        if not self._socket:
            raise RuntimeError('Socket not available')
        # Beware: the arguments of a log() call are evaluated even when the
        # verbosity level is too low, so check the level before building the
        # (potentially large) hex dump.
        if log and self.verbose >= 2: self.log(2, 'sending:', msg)
        data = cbor2.dumps(msg, encoders=self._encoders_cache, default=self._encode_default)
        if log and self.verbose >= 3: self.log(3, f'CBOR payload: {data.hex(" ")}')
        self._socket.send(data, flags=0 if block else zmq.DONTWAIT)
        self.lastSendTime = self.time()
        if log and self.verbose >= 2: self.log(2, 'sent')

    def _reply(self, msg: Dict[str, Any]) -> None:
        """Send a reply, tolerating a remote side that is already gone."""
        try:
            self.send(msg, log=False)
        except Exception as e:
            self.log(1, 'could not send reply:', e)

    def recv(self, *, block: bool = True, log: bool = True) -> Optional[Dict[str, Any]]:
        """
        Low-level receiver: receives and CBOR-decodes a message.

        @param block: if True, blocks on the socket until a message is available;
                      if False, uses NOBLOCK.
        Returns the decoded dict, or None if no message was available (only when block=False).
        """
        if not self._socket:
            raise RuntimeError('Socket not available')
        if block:
            # Blocking receive: poll (event driven, so it returns immediately
            # when a message arrives) in order to notice when the remote side
            # disappears without being able to send anything. ZMQ does not
            # signal a peer disconnection, so silence is the only indication.
            while True:
                if self._socket.poll(timeout=1000):
                    data = self._socket.recv(flags=zmq.NOBLOCK)
                    break
                # Keep the connection alive while we are waiting for a reply
                # (e.g. while the simulation is paused, or while Lua is busy):
                # otherwise the remote side would consider us gone after its
                # own liveness timeout.
                self._sendKeepAlive()
                if not self.isRemoteAlive():
                    raise RemoteGoneError('the remote side is not responding anymore')
        else:
            try:
                data = self._socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                return None
        if log and self.verbose >= 2: self.log(2, f'received {len(data)} bytes')
        if log and self.verbose >= 3: self.log(3, f'CBOR payload: {data.hex(" ")}')
        try:
            msg = cbor2.loads(data, tag_hook=self._tag_hook)
        except Exception as e:
            # Never drop the message silently: the peer sent a reply that was
            # received (and consumed) here, so the caller would otherwise wait
            # forever for a response that will never come again.
            raise RuntimeError(f'invalid CBOR data: {e!s}') from e
        if log and self.verbose >= 2: self.log(2, 'received:', msg)
        self.lastRecvTime = self.time()
        return msg

    def _tag_hook(self, tag: cbor2.CBORTag, immutable: bool) -> Any:
        """
        CBOR tag hook for decoding special types (numpy arrays, sim objects, etc.)
        This is identical to the original ZMQRemoteAPI.
        """

        if tag.tag == sim.Matrix.CBOR_TAG:
            return sim.Matrix.fromcbor(tag)
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
            # np.ndarray provided for convenience -> sim.Matrix:
            np.ndarray: lambda e, v: e.encode(v.view(sim.Matrix).__cbor__())
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


class Env(dict):
    """A dict suitable as globals() for eval()/exec(), layering:

        virtuals  ->  locally-set names  ->  fallback (e.g. caller's globals())

    Reads hit the first layer that has the name.  Writes go to the virtual
    setter if the name is virtual, otherwise into this dict (shadowing the
    fallback).  The fallback is held by reference and never mutated; `del`
    / `pop` only affect locally-set names.
    """

    def __init__(self, fallback=None, virtuals=None, initial=None):
        super().__init__()
        self._fallback = fallback if fallback is not None else {}
        self._virtuals = dict(virtuals or {})          # name -> (getter, setter)
        if initial:
            super().update(initial)
        for name in self._virtuals:                    # virtuals are never stored
            super().pop(name, None)

    def _is_virtual(self, key):
        return key in self._virtuals

    # ---- read paths -----------------------------------------------------
    def __getitem__(self, key):
        if self._is_virtual(key):
            return self._virtuals[key][0]()
        try:
            return super().__getitem__(key)
        except KeyError:
            return self._fallback[key]                 # may KeyError -> NameError

    def __contains__(self, key):
        return (self._is_virtual(key)
                or super().__contains__(key)
                or key in self._fallback)

    def get(self, key, default=None):
        if self._is_virtual(key):
            return self._virtuals[key][0]()
        if super().__contains__(key):
            return super().__getitem__(key)
        try:
            return self._fallback[key]
        except KeyError:
            return default

    def setdefault(self, key, default=None):
        if self._is_virtual(key):
            return self._virtuals[key][0]()
        if super().__contains__(key):
            return super().__getitem__(key)
        try:
            return self._fallback[key]
        except KeyError:
            pass
        super().__setitem__(key, default)
        return default

    # ---- write paths ----------------------------------------------------
    def __setitem__(self, key, value):
        if self._is_virtual(key):
            self._virtuals[key][1](value)
            return
        super().__setitem__(key, value)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def __delitem__(self, key):
        if self._is_virtual(key):
            raise KeyError(key)
        super().__delitem__(key)                       # only locally-set names

    def pop(self, key, *args):
        if self._is_virtual(key):
            if args:
                return args[0]
            raise KeyError(key)
        if super().__contains__(key):
            return super().pop(key)
        if args:
            return args[0]
        raise KeyError(key)

    def flush(self):
        """STORE_GLOBAL calls PyDict_SetItem directly, so a virtual name
        written from inside a function lands in the real dict.  Route any
        such leaked value to the setter and remove the raw entry."""
        for name, (_, setter) in self._virtuals.items():
            if super().__contains__(name):
                setter(super().pop(name))


class PythonWrapper:
    def __init__(self, _globals: dict[str, Any], *, port: int, client_id: str, sysCalls: list[str], verbose: int = 0, barePrint: bool = False, **kwargs):
        self._g = _globals
        import sim
        self._env = Env(_globals, {
            'cur': (
                lambda: sim.app.current,
                lambda v: setattr(sim.app, 'current', v),
            ),
            'sel': (
                lambda: sim.scene.selection,
                lambda v: setattr(sim.scene, 'selection', v),
            ),
            'sel1': (
                lambda: sim.scene.selection[-1] if len(sim.scene.selection) > 0 else None,
                lambda v: setattr(sim.scene, 'selection', [v]),
            ),
            'h': (
                lambda: sim.scene.getObject,
                lambda v: None,
            ),
        })
        self.rapi = ZMQRemoteAPI(
            name = f'pythonWrapper-py-{port}-{client_id}',
            server = False,
            port = port,
            verbose = verbose,
        )
        if barePrint is False:
            self.rapi.print = lambda *args: self.rapi.callEx(None, '__print', tuple(map(str, args)), log=False)

        self.sysCalls = sysCalls
        sim.Object._callMethod = self.rapi.call
        # The Lua side may be halted for arbitrary amounts of time (popup menu,
        # modal dialog, edit mode): never treat its silence as death. This
        # process is killed by the Lua side when the script is cleaned up.
        self.rapi.remoteSilenceTimeout = 0
        # Let 'myFunc#ext' calls reach a plain Python function defined by the
        # script, without having to register it explicitly (registered callbacks
        # still take precedence).
        self.rapi.resolveCallable = self._resolveGlobal
        self.rapi.log(1, 'initializing...')

    def _resolveGlobal(self, name: str) -> Any:
        """
        Looks up a (possibly dotted) callable in the script's namespace, e.g.
        'myFunc' or 'a.b.myFunc'. Returns None if it does not exist or is not
        callable.
        """
        obj: Any = self._g
        for part in name.split('.'):
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                obj = getattr(obj, part, None)
            if obj is None:
                return None
        return obj if callable(obj) else None

    def registerSysCalls(self):
        self.rapi.log(1, 'registering sysCalls...')
        import inspect
        for name in self.sysCalls:
            func = self._g.get(name)
            if not callable(func):
                continue
            if inspect.iscoroutinefunction(func):
                raise TypeError(
                    f'"{name}" must be a regular function, not a coroutine function. '
                    f'Threading is provided by Lua: use sim.self.step() (or sim.step()) to yield.')
            self.rapi.log(2, f'registering sysCall {name}...')
            self.rapi.registerCallback(name, func, False)

    def step(self):
        """
        Suspend the calling sysCall_thread until the next simulation step.

        This is a regular blocking call: Lua's thread coroutine suspends itself
        (exactly as sim.self:step() does in Lua) and answers the call when it is
        resumed in a later simulation step. While suspended, other calls coming
        from Lua (e.g. sysCall_actuation) are still served.
        """
        if not self._callStack or self._callStack[-1] != 'sysCall_thread':
            raise RuntimeError('sim.step()/sim.sleep() can only be used from within sysCall_thread')
        sim.self.step()

    def sleep(self, duration):
        """Suspend the calling sysCall_thread for the given simulation duration."""
        if not self._callStack or self._callStack[-1] != 'sysCall_thread':
            raise RuntimeError('sim.step()/sim.sleep() can only be used from within sysCall_thread')
        end = sim.scene.simulation.time + duration
        while sim.scene.simulation.time < end:
            sim.self.step()

    def ready(self):
        self.rapi.log(1, 'ready')
        self.rapi.call(None, '__ready')

    def spin(self):
        self.ready()
        self.rapi.log(1, 'spinning')
        while True:
            # Drain anything that is already queued first: this also refreshes
            # the remote-alive timestamp after e.g. a long-running
            # sysCall_thread, during which no message was read for a while.
            self.rapi.spinSome()
            # Blocking wait (instead of a busy-wait): callbacks are served as
            # soon as they arrive, but an idle script does not consume CPU.
            #
            # Note: we do NOT exit when isRemoteAlive() returns False. The Lua
            # side may legitimately be silent for a while (the application can
            # be in an edit mode or in a modal dialog, where sysCall callbacks
            # are not dispatched, or it can be busy with a long operation).
            # This process' lifetime is governed by the Lua side, which kills it
            # in __pythonWrapper_cleanup().
            self.rapi.spinSome(0.1)
            sys.stdout.flush()
            sys.stderr.flush()

    def registerCmdInterface(self):
        self.rapi.log(1, 'registering cmd intefrace...')
        self.rapi.registerCallback('__pythonWrapper_evalExec', self.evalExec, False)
        self.rapi.registerCallback('__pythonWrapper_evalExecRet', self.evalExecRet, False)
        self.rapi.registerCallback('__pythonWrapper_getCompletion', self.getCompletion, False)
        self.rapi.registerCallback('__pythonWrapper_getCalltip', self.getCalltip, False)

    def evalExec(self, theStr):
        try:
            ret = eval(theStr, self._env)
            if ret is not None:
                self.rapi.print(ret)
        except SyntaxError:
            exec(theStr, self._env)

    def evalExecRet(self, theStr):
        try:
            ret = eval(theStr, self._env)
            return ret
        except SyntaxError:
            exec(theStr, self._env)

    def getCompletion(self, input, pos):
        import functools, re
        ret = []
        if pos == len(input):
            if m := re.search(r'[_\w][_\w\.]*$', input):
                *parts, last = m.group().split('.')
                obj = functools.reduce(lambda o, k: None if o is None else self._g.get(k) if o == self._g else getattr(o, k, None), parts, self._g)
                for k in ([] if obj is None else self._g if obj == self._g else dir(obj)):
                    if k.startswith(last) and len(k) > len(last):
                        ret.append(k[len(last):])
        return ret

    def getCalltip(self, input, pos, scriptHandle):
        def getCallContexts(code, pos):
            from io import BytesIO
            from tokenize import tokenize, TokenInfo, TokenError
            import tokenize as T

            def lpos(code, row, col):
                if row > 1:
                    i = code.index('\n')
                    return i + 1 + lpos(code[i+1:], row-1, col)
                else:
                    return col

            tokens = []
            try:
                for t in tokenize(BytesIO(code.encode('utf-8')).readline):
                    tokens.append(t)
            except TokenError:
                pass

            def merge_namedotname(tokens):
                for i, (t1, t2, t3) in enumerate(zip(tokens, tokens[1:], tokens[2:])):
                    if t1.type == T.NAME and t2.type == T.OP and t2.string == '.' and t3.type == T.NAME:
                        merged = TokenInfo(T.NAME, t1.string + '.' + t3.string, t1.start, t3.end, t1.line + t2.line + t3.line)
                        return merge_namedotname(tokens[:i] + [merged] + tokens[i+3:])
                return tokens
            tokens = merge_namedotname(tokens)

            tokens = list(filter(lambda t: t.type == T.NAME or (t.type == T.OP and t.string in '()'), tokens))

            ranges = {}
            stack = []
            last_name = None
            for t in tokens:
                if t.string == '(':
                    stack.append(last_name)
                elif t.string == ')':
                    if not stack: return []
                    tstart = stack.pop()
                    if tstart:
                        ranges[tstart.string] = (lpos(code, *tstart.start), lpos(code, *t.end))
                else:
                    last_name = t
            # flush stack:
            end = len(code)
            while stack:
                tstart = stack.pop()
                if tstart:
                    ranges[tstart.string] = (lpos(code, *tstart.start), end)

            ret = []
            for name, (start, end) in ranges.items():
                if start <= pos <= end:
                    ret.append(name)
            return ret

        cc = getCallContexts(input, pos)
        if cc:
            return 'not implemented'  #sim.getApiInfo(-1, cc[0])
        else:
            return ''
