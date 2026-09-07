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
                self.handle_request(rep)

    def register_callback(self, func_name: str, func: Callable) -> None:
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
        """Process a single incoming request (call or registerCallback)."""
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
        if not self.server:
            raise RuntimeError('handle_requests should be called only from server')
        while True:
            req = self.recv(block=False)
            if req is None:
                break
            self.handle_request(req)

    def send(self, msg: dict[str, Any]) -> None:
        if not self._socket:
            raise RuntimeError('Socket not available')
        self.log(2, 'sending:', msg)
        data = cbor2.dumps(msg)
        self._socket.send(data)
        self.log(2, 'sent')

    def recv(self, block: bool = True) -> dict[str, Any] | None:
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


if __name__ == '__main__':
    rapi = ZMQRemoteAPI({'name': 'handshake', 'server': False})
    port = rapi.call(None, 'getPort', rapi.client_id)
    rapi = ZMQRemoteAPI({'name': 'client', 'server': False, 'port': port})

    '''
    def cb(x):
        return rapi.call('test2') + '-CB-' + x

    rapi.register_callback('cb', cb)
    result = rapi.call('testWithCallback', 'cb')
    print(result)
    '''

    sim.Object._callMethod = rapi.call

    print(sim.self.handle)
    print(sim.scene)
    print(sim.scene.getObject)
    Floor = sim.scene.getObject('Floor')
    print(Floor)
    print(Floor.objectType)
    print(getattr(Floor, 'metaInfo.superClass'))
    print(Floor.metaInfo.superClass)
    print(Floor.name)
    print(Floor.position)
    print(Floor.quaternion)
    print(Floor.pose)
    print(Floor.getPropertyInfo('bla', {'noError': True}))
    print(Floor.getPropertyName(0))
    print(Floor.getPropertyName(100000))
    print(Floor.xxx)
