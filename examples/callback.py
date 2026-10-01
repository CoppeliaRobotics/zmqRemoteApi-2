import sim
from sim.zmqremoteapi import ZMQRemoteAPI

port = ZMQRemoteAPI().call(None, 'getPort')
rapi = ZMQRemoteAPI({'port': port})
sim.Object._callMethod = rapi.call

def cb():
    return 'cb'

def cb2(f):
    def cb3():
        return 'cb3'
    return 'cb2-' + f(cb3)

print('create object...')
obj = sim.app.createObject({'type': 'test'})
print(obj)
print('invoke obj.testCallback(cb)')
res = obj.testCallback(cb)
print('result:', res)
print('invoke obj.testReentrantCallback(cb2)')
res = obj.testReentrantCallback(cb2)
print('result:', res)
