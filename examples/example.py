try:
    import sim
    from sim.zmqremoteapi import ZMQRemoteAPI
except ModuleNotFoundError:
    raise SystemExit('CoppeliaSim libraries not found. Make sure those can be found via Python\'s path, e.g.: PYTHONPATH=/path/to/coppeliaSim/python python3 example.py')

# handshake to get a port number:
rapi = ZMQRemoteAPI({'name': 'handshake', 'server': False})
port = rapi.call(None, 'getPort')
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

print('sim.self.handle:', sim.self.handle)
print('sim.scene:', sim.scene)
print('calling sim.scene.getObject("Floor")...')
Floor = sim.scene.getObject('Floor')
print('sim.scene.getObject("Floor"):', Floor)
print('Floor.objectType:', Floor.objectType)
print('Floor.metaInfo.superClass (via getattr()):', getattr(Floor, 'metaInfo.superClass'))
print('Floor.metaInfo.superClass (via dot notation):', Floor.metaInfo.superClass)
print('Floor.name:', Floor.name)
print('Floor.position:', Floor.position)
print('Floor.quaternion:', Floor.quaternion)
print('Floor.pose:', Floor.pose)
print('Floor.getPropertyInfo("xxx", {"noError": True}):', Floor.getPropertyInfo('bla', {'noError': True}))
print('Floor.getPropertyName(0):', Floor.getPropertyName(0))
print('Floor.getPropertyName(100000):', Floor.getPropertyName(100000))
print('Floor.xxx: (will cause an error)')
print(Floor.xxx)
