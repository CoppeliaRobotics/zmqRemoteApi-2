import 'sim-2'
import 'sim.ZMQAsyncRemoteAPI-2'

assert(WORKER_PORT, 'WORKER_PORT not defined')
IDLE_TIMEOUT = IDLE_TIMEOUT or 10 -- seconds before termination after no received messages

function noop()
end

function sysCall_init()
    rapi = sim.ZMQAsyncRemoteAPI{
        name = 'worker-' .. WORKER_PORT,
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

        -- terminate if inactive:
        local termTime = rapi.lastRecvTime + IDLE_TIMEOUT
        if sim.app.systemTime >= termTime then
            rapi:log(1, 'terminating worker script (handle=' .. sim.self.handle ..') because of inactivity')
            sim.self:remove()
        end

        sim.self:yield()
    end
end

function sysCall_cleanup()
    rapi:cleanup()
    rapi = nil
end
