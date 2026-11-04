import argparse
argparser = argparse.ArgumentParser()
argparser.add_argument("-v", "--verbose", type=int, default=0, metavar="N", help="verbosity level (default: 0)")
args = argparser.parse_args()

import sim
from sim.zmqremoteapi import ZMQRemoteAPI
from time import time

port = ZMQRemoteAPI().call(None, 'spawnWorker', {'verbose': args.verbose})
print('port:', port)

rapi = ZMQRemoteAPI(name = 'client', server = False, port = port, verbose = args.verbose)

sim.Object._callMethod = rapi.call

print('starting simulation...')
sim.scene.simulation.start()
while sim.scene.simulation.state != 17: rapi.spinSome()
print('started simulation.')

print(f'simulation time = {sim.scene.simulation.time:.3f}')
for i in range(5):
    if sim.scene.simulation.time > 5: break
    print(f'simulation time = {sim.scene.simulation.time:.3f}')
    rapi.spinSome()

print('stopping simulation...')
sim.scene.simulation.stop()
while sim.scene.simulation.state != 0: rapi.spinSome()
print('stopped simulation.')
