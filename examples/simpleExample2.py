import sim
from sim.zmqremoteapi import ZMQRemoteAPI

rapi = ZMQRemoteAPI()
sim.Object._callMethod = rapi.call

done = False

def s_actuation():
    # this runs synchronously with to CoppeliaSim
    t = sim.scene.simulation.time
    sim.app.logInfo(f'From actuation section, simulation time: {t:.2f} [s]')
    print(f'From actuation section, simulation time: {t:.2f} [s]')

def s_thread():
    # note: the thread section is also serviced when simulation is not running, unlike most other callback functions!
    global done
    while sim.scene.simulation.state == sim.simulationState.stopped:
        sim.self.step() # a simulation start command is not immediately executed
    while sim.scene.simulation.time < 5:
        # this runs synchronously with to CoppeliaSim
        t = sim.scene.simulation.time
        sim.app.logInfo(f'From thread section, simulation time: {t:.2f} [s]')
        print(f'From thread section, simulation time: {t:.2f} [s]')
        sim.self.step()
    sim.scene.simulation.stop()
    done = True

rapi.registerCallback('sysCall_actuation', s_actuation, True)
sim.self.setStepping(True) # run the thread section synchronously with CoppeliaSim
sim.scene.simulation.start()

# Register the thread always *last*
rapi.registerCallback('sysCall_thread', s_thread, True)

while not done:
    rapi.spinSome(1.0) # let remaining calls/commands be serviced
