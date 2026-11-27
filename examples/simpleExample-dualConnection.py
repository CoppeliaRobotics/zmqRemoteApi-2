import sim
from sim.zmqremoteapi import ZMQRemoteAPI

portInstance2 = 24899

simInstance1 = ZMQRemoteAPI(name='simInstance1')
simInstance2 = ZMQRemoteAPI(name='simInstance2', broker_port=portInstance2)

# The work happens in instance 2: bare sim.Object(...) must route there.
# (simInstance1 was the first client, so it got the automatic default; override.)
sim.Object._callMethod = simInstance1.call

sim.scene.simulation.start()
simInstance2.Object(sim.handle.scene).simulation.start()

def s_sensing():
    sim.app.logInfo('Hello to instance 1')
    simInstance2.Object(sim.handle.app).logInfo('Hello to instance 2')

simInstance1.registerCallback('sysCall_sensing', s_sensing, True)

simInstance1.spin() # serves instance 1's callbacks

simInstance2.cleanup()
simInstance1.cleanup()
