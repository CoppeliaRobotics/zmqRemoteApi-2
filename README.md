# ZMQ Remote API 2 for CoppeliaSim

This is the version 2 of the ZMQ Remote API for CoppeliaSim

### Lua

Lua example:

```lua
import 'sim-2'
import 'sim.ZMQRemoteAPI-2'

function cb(arg)
    return 'CALLBACK' .. arg
end

function sysCall_init()
end

function sysCall_thread()
    -- handshake to spawn worker and get the port number:
    rapi = sim.ZMQRemoteAPI{name = '/clientTest[handshake]'}
    local port = rapi:call(nil, 'spawnWorker', rapi.clientID)
    -- connect to port:
    rapi = sim.ZMQRemoteAPI{
        name = '/clientTest',
        port = port,
        verbose = 2,
    }

    --rapi:log(0, 'calling "test"...')
    --local result = rapi:call(nil, 'test')
    --rapi:log(0, 'result of calling "test":', result)

    rapi:registerCallback('cb')
    rapi:log(0, 'calling "testWithCallback"...')
    local result = rapi:call(nil, 'testWithCallback', 'cb')
    rapi:log(0, 'result of calling "testWithCallback":', result)
end

function sysCall_cleanup()
    rapi:cleanup()
    rapi = nil
end
```

### Python

Python example:

```python
from sim.zmqremoteapi import ZMQRemoteAPI

# handshake to spawn worker and get the port number:
rapi = ZMQRemoteAPI({'name': 'handshake', 'server': False})
port = rapi.call(None, 'spawnWorker', rapi.client_id)
# connect to port:
rapi = ZMQRemoteAPI({'name': 'client', 'server': False, 'port': port})

'''
def cb(x):
    return rapi.call(None, 'test2') + '-CB-' + x

rapi.register_callback('cb', cb)
result = rapi.call(None, 'testWithCallback', 'cb')
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
```
