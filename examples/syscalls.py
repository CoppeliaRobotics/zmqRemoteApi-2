import argparse
argparser = argparse.ArgumentParser()
argparser.add_argument("-v", "--verbose", type=int, default=0, metavar="N", help="verbosity level (default: 0)")
args = argparser.parse_args()

import sim
from sim.zmqremoteapi import ZMQRemoteAPI

port = ZMQRemoteAPI().call(None, 'spawnWorker')
rapi = ZMQRemoteAPI(port = port, verbose = args.verbose)
sim.Object._callMethod = rapi.call

def s_init():
    sim.scene.simulation.start()

def s_sensing():
    print(f'sensing phase (t={sim.scene.simulation.time:.3f})...')
    if sim.scene.simulation.time > 5:
        sim.scene.simulation.stop()

def s_actuation():
    print(f'actuation phase (t={sim.scene.simulation.time:.3f})...')

rapi.registerCallback('sysCall_sensing', s_sensing, True)
rapi.registerCallback('sysCall_actuation', s_actuation, True)
s_init()
while 1: rapi.spinSome() # keep client alive & running
