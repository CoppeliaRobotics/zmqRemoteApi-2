local sim = require 'sim-2'
local simZMQ
local cbor

sim.ZMQRemoteAPI = require 'sim.ZMQRemoteAPI-2'

function sysCall_info()
    return {
        menu = 'Connectivity\nZMQ remote API 2 server',
    }
end

function sysCall_init()
    workers = {}
    rapi_broker = sim.ZMQRemoteAPI{
        name = 'server',
        server = true,
    }
    rapi_broker:registerCallback('getPort', getPort)
    rapi_broker:log(1, 'server started')
end

function sysCall_thread()
    while true do
        rapi_broker:handleRequests()
        sim.self:yield()
    end
end

function sysCall_cleanup()
    for client_id, worker in pairs(workers) do
        worker.script:remove()
    end
    workers = {}
    rapi_broker:cleanup()
    rapi_broker = nil
end

function getPort()
    worker = {}
    worker.port = nextWorkerPort or 24500
    worker.script = sim.app:createObject{
        type = 'script',
        language = 'lua',
        addOnMenuPath = 'zmqRemoteApiServerWorker-' .. worker.port,
        code =
            "WORKER_PORT = " .. worker.port .. "\n" ..
            "require 'sim.ZMQRemoteAPI-2-worker'",
    }
    rapi_broker:log(1, 'spawning worker for client at port ' .. worker.port, worker.script)
    worker.script:init()
    workers[worker.port] = worker
    nextWorkerPort = worker.port + 1
    return worker.port
end
