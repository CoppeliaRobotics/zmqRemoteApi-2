import sim
from sim.zmqremoteapi import ZMQRemoteAPI
from time import time

rapi = ZMQRemoteAPI({'name': 'client-handshake', 'server': False, 'verbose': 0})
port = rapi.call(None, 'getPort')
print('port:', port)

rapi = ZMQRemoteAPI({'name': 'client', 'server': False, 'port': port, 'verbose': 0})

sim.Object._callMethod = rapi.call

scene = sim.scene

def s_sensing():
    print(f'sensing phase (t={scene.simulation.time:.3f})...')

def s_actuation():
    print(f'actuation phase (t={scene.simulation.time:.3f})...')

rapi.registerCallback('sysCall_sensing', s_sensing, True)
rapi.registerCallback('sysCall_actuation', s_actuation, True)

def noop():
    if rapi.lastSendTime + 5 < time():
        # keep worker alive
        rapi.call(None, 'noop')
    rapi.spinSome(0.010)

print('starting simulation...')
scene.simulation.start()
while scene.simulation.state != 17: noop()
print('started simulation.')

print(f'simulation time = {scene.simulation.time:.3f}')
for i in range(5):
    if scene.simulation.time > 5: break
    print(f'simulation time = {scene.simulation.time:.3f}')
    noop()

print('stopping simulation...')
scene.simulation.stop()
while scene.simulation.state != 0: noop()
print('stopped simulation.')

while 1: noop() # keep client alive & running
