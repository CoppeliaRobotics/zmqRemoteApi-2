local sim = require 'sim-2'

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
        verbose = sim.app.customData['zmqRemoteApi2.verbose'] or 0,
    }
    rapi_broker.keepAliveInterval = 0 -- broker doesn't need keep-alive
    rapi_broker:registerCallback('getPort', getPort)
    rapi_broker:registerCallback('spawnWorker', spawnWorker)
    rapi_broker:log(1, 'server started')
end

function sysCall_thread()
    while true do
        rapi_broker:spinSome()
        sim.self:yield()
    end
end

function sysCall_addOnScriptSuspend()
    return {cmd = 'cleanup'}
end

function sysCall_cleanup()
    for client_id, worker in pairs(workers) do
        rapi_broker:log(2, 'removing worker script', client_id, worker.script)
        if worker.script:isValid() then
            worker.script:remove()
        end
    end
    workers = {}
    rapi_broker:cleanup()
    rapi_broker:log(1, 'server stopped')
    rapi_broker = nil
end

function getPort()
    local port = nextWorkerPort or 24500
    local simZMQ = require 'simZMQ'
    -- Only hand out a port that can actually be bound: a worker that fails to
    -- bind would leave its client waiting forever, without any error.
    while port < 65535 do
        nextWorkerPort = port + 1
        local ctx = simZMQ.ctx_singleton()
        local sock = simZMQ.socket(ctx, simZMQ.REP)
        local available = (simZMQ.bind(sock, 'tcp://*:' .. port) == 0)
        simZMQ.close(sock)
        if available then
            return port
        end
        port = port + 1
    end
    error('no free port available for a worker')
end

-- Values of the `script.state` property (same as sim.scriptState):
--   0 unloaded, 1 uninitialized, 2 initialized (running), 3 ended, 4 suspended
local SCRIPT_STATE_INITIALIZED = 2
local SCRIPT_STATE_NAMES = {
    [0] = 'unloaded',
    [1] = 'uninitialized',
    [2] = 'initialized',
    [3] = 'ended',
    [4] = 'suspended',
}

function spawnWorker(opts)
    opts = opts or {}
    local worker = {}
    if opts.port then
        worker.port = opts.port
    else
        worker.port = getPort()
        opts.port = worker.port
    end
    worker.script = sim.app:createObject{
        type = 'script',
        language = 'lua',
        addOnMenuPath = 'zmqRemoteApiServerWorker-' .. worker.port,
        code =
            "WORKER_OPTS = " .. table.tostring(opts) .. "\n" ..
            "require 'sim.ZMQRemoteAPI-2-worker'",
    }
    rapi_broker:log(1, 'spawning worker for client at port ' .. worker.port, worker.script)
    worker.script:init() -- synchronous for another script: sysCall_init runs now

    -- init() is synchronous here, so a worker that is still serviceable must be
    -- in the 'initialized' state. If it failed (e.g. its sysCall_init raised),
    -- report that to the client instead of letting it wait forever for a worker
    -- that will never answer.
    local ok, state = pcall(function() return worker.script.state end)
    if (not ok) or (state ~= SCRIPT_STATE_INITIALIZED) then
        local s = 'unknown'
        if ok then
            s = tostring(state) .. ' (' .. (SCRIPT_STATE_NAMES[state] or 'unknown') .. ')'
        end
        error('the worker script failed to initialize (state=' .. s ..
              '); check the simulator console for errors related to the worker script (port ' ..
              worker.port .. ')')
    end

    workers[worker.port] = worker
    return worker.port
end
