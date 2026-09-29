import sim
from sim.zmqasyncremoteapi import ZMQAsyncRemoteAPI
from time import time

rapi = ZMQAsyncRemoteAPI({'name': 'client-handshake', 'server': False, 'verbose': 0})
port = rapi.call(None, 'getPort')
print('port:', port)

rapi = ZMQAsyncRemoteAPI({'name': 'client', 'server': False, 'port': port, 'verbose': 0})

sim.Object._callMethod = rapi.call

scene = sim.scene

def s_sensing():
    print(f'sensing phase (t={scene.simulation.time})...')

rapi.register_callback('sysCall_sensing', s_sensing)

rapi.register_callback('noop', lambda: None)

def noop():
    if rapi.last_send_time + 5 < time():
        # keep worker alive
        rapi.call(None, 'noop')
    rapi.poll(10)

print('starting simulation...')
scene.simulation.start()
while scene.simulation.state != 17: noop()
print('started simulation.')

print(f'simulation time = {scene.simulation.time}')
for i in range(5):
    if scene.simulation.time > 5: break
    print(f'simulation time = {scene.simulation.time}')
    noop()

print('stopping simulation...')
scene.simulation.stop()
while scene.simulation.state != 0: noop()
print('stopped simulation.')

while 1: noop() # keep client alive & running
