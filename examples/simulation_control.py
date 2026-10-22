import argparse
argparser = argparse.ArgumentParser()
argparser.add_argument("-v", "--verbose", type=int, default=0, metavar="N", help="verbosity level (default: 0)")
args = argparser.parse_args()

import sim
from sim.zmqremoteapi import ZMQRemoteAPI
from time import time

port = ZMQRemoteAPI().call(None, 'spawnWorker')
print('port:', port)

rapi = ZMQRemoteAPI(name = 'client', server = False, port = port, verbose = args.verbose)

sim.Object._callMethod = rapi.call

scene = sim.scene

def s_sensing():
    print(f'sensing phase (t={scene.simulation.time:.3f})...')

def s_actuation():
    print(f'actuation phase (t={scene.simulation.time:.3f})...')

rapi.registerCallback('sysCall_sensing', s_sensing, True)
rapi.registerCallback('sysCall_actuation', s_actuation, True)

print('starting simulation...')
scene.simulation.start()
while scene.simulation.state != 17: rapi.spinSome()
print('started simulation.')

print(f'simulation time = {scene.simulation.time:.3f}')
for i in range(5):
    if scene.simulation.time > 5: break
    print(f'simulation time = {scene.simulation.time:.3f}')
    rapi.spinSome()

print('stopping simulation...')
scene.simulation.stop()
while scene.simulation.state != 0: rapi.spinSome()
print('stopped simulation.')

while 1: rapi.spinSome() # keep client alive & running
