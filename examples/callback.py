import sim
from sim.zmqremoteapi import ZMQRemoteAPI

port = ZMQRemoteAPI().call(None, 'getPort')
rapi = ZMQRemoteAPI({'port': port})
sim.Object._callMethod = rapi.call

def cb():
    return 'xxx'

print('create object...')
obj = sim.app.createObject({'type': 'test'})
print(obj)
print('obj.foo', obj.foo)
print('invoke obj.bar(cb)')
res = obj.bar(cb)
print('result:', res)
