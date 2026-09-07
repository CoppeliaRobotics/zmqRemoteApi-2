try:
    import sim
    from sim.zmqremoteapi import ZMQRemoteAPI
except ModuleNotFoundError:
    raise SystemExit('CoppeliaSim libraries not found. Make sure those can be found via Python\'s path, e.g.: PYTHONPATH=/path/to/coppeliaSim/python python3 example.py')

# handshake to get a port number:
rapi = ZMQRemoteAPI({'name': 'handshake', 'server': False})
port = rapi.call(None, 'getPort', rapi.client_id)
# connect to port:
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
