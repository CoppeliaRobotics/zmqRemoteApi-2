import argparse
argparser = argparse.ArgumentParser()
argparser.add_argument("-v", "--verbose", type=int, default=0, metavar="N", help="verbosity level (default: 0)")
args = argparser.parse_args()

import sim
from sim.zmqremoteapi import ZMQRemoteAPI

port = ZMQRemoteAPI().call(None, 'spawnWorker')
rapi = ZMQRemoteAPI(port = port, verbose = args.verbose)
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
