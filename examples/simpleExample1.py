import sim
from sim.zmqremoteapi import ZMQRemoteAPI

rapi = ZMQRemoteAPI()

sim.scene.simulation.start()
while (t := sim.scene.simulation.time) < 5:
    # this loop runs asynchronously relative to CoppeliaSim
    sim.app.logInfo(f'Simulation time: {t:.2f} [s]')
    print(f'Simulation time: {t:.2f} [s]')

sim.scene.simulation.stop()
