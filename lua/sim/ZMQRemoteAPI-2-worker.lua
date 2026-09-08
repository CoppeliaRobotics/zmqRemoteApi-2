import 'sim-2'
import 'sim.ZMQAsyncRemoteAPI-2'

assert(CLIENT_ID, 'CLIENT_ID not defined')
assert(WORKER_PORT, 'WORKER_PORT not defined')

function sysCall_init()
    rapi = sim.ZMQAsyncRemoteAPI{
        name = 'worker-' .. CLIENT_ID,
        port = WORKER_PORT,
        server = true,
        verbose = 2,
    }
    rapi.callMethod = sim.callMethod
    rapi:log(1, 'spawned worker script (handle=' .. sim.self.handle ..')')
end

function sysCall_thread()
    while true do
        rapi:processRequests(10)
        sim.self:yield()
    end
end

function sysCall_cleanup()
    rapi:cleanup()
    rapi = nil
end
