import 'sim-2'
import 'sim.ZMQRemoteAPI-2'

assert(WORKER_PORT, 'WORKER_PORT not defined')

function sysCall_init()
    rapi = sim.ZMQRemoteAPI{
        name = 'worker-' .. WORKER_PORT,
        port = WORKER_PORT,
        server = true,
        verbose = 2,
    }
    rapi.callMethod = sim.callMethod
    rapi:log(1, 'spawned worker script (handle=' .. sim.self.handle ..')')
end

function sysCall_thread()
    while rapi:isRemoteAlive() do
        rapi:spinSome()
        sim.self:yield()
    end
    rapi:log(1, 'terminating worker script (handle=' .. sim.self.handle ..') because of inactivity')
    sim.self:remove()
end

function sysCall_cleanup()
    rapi:cleanup()
    rapi = nil
    sim.self:remove()
end
