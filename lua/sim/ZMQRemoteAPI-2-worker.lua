import 'sim-2'
import 'sim.ZMQRemoteAPI-2'

assert(CLIENT_ID, 'CLIENT_ID not defined')
assert(WORKER_PORT, 'WORKER_PORT not defined')

function test()
    rapi:log(0, 'function "test" called')
    return 'TEST1'
end

function testWithCallback(cb)
    rapi:log(0, 'function "testWithCallback" called')
    return _G[cb]('TwCB')
end

function test2()
    rapi:log(0, 'function "test2" called')
    return 'TEST2'
end

function sysCall_init()
    rapi = sim.ZMQRemoteAPI{
        name = '/zmqRemoteApiServer[worker-' .. CLIENT_ID .. ']',
        port = WORKER_PORT,
        server = true,
        verbose = 2,
    }
    rapi.callMethod = sim.callMethod
    rapi:log(1, 'spawned ' .. rapi.name)
end

function sysCall_thread()
    while true do
        rapi:handleRequests()
        sim.self:yield()
    end
end

function sysCall_cleanup()
    rapi:cleanup()
    rapi = nil
end
