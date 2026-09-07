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
    rapi_broker = sim.ZMQRemoteAPI{
        name = '/zmqRemoteApiServer[broker]',
        server = true,
    }
    workers = {}
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

function getPort(client_id)
    local worker = workers[client_id]
    if not worker then
        worker = {}
        worker.port = nextWorkerPort or 24500
        worker.script = sim.app:createObject{
            type = 'detachedScript',
            language = 'lua',
            addOnMenuPath = 'zmqRemoteApiServerWorker-' .. client_id,
            code =
                "CLIENT_ID = " .. client_id .. "\n" ..
                "WORKER_PORT = " .. worker.port .. "\n" ..
                "require 'sim.ZMQRemoteAPI-2-worker'",
        }
        rapi_broker:log(1, 'spawning worker for client ' .. client_id, worker.script)
        worker.script:init()
        workers[client_id] = worker
        nextWorkerPort = worker.port + 1
    end
    return worker.port
end
