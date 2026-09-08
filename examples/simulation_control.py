import sim
from sim.zmqremoteapi import ZMQRemoteAPI
from sim.zmqasyncremoteapi import ZMQAsyncRemoteAPI

rapi = ZMQRemoteAPI({'name': 'handshake', 'server': False})
port = rapi.call(None, 'getPort', rapi.client_id)

rapi = ZMQAsyncRemoteAPI({'name': 'client', 'server': False, 'port': port})

sim.Object._callMethod = rapi.call

scene = sim.scene

def s_sensing():
    print(f'sensing phase (t={scene.simulation.time})...')

rapi.register_callback('sysCall_sensing', s_sensing)

noop = lambda: rapi.poll(10)

print('starting simulation...')
scene.simulation.start()
while scene.simulation.state != 17: noop()
print('started simulation.')

print(f'simulation time = {scene.simulation.time}')
while scene.simulation.time < 0.1:
    print(f'simulation time = {scene.simulation.time}')
    noop()

print('stopping simulation...')
scene.simulation.stop()
while scene.simulation.state != 0: noop()
print('stopped simulation.')
