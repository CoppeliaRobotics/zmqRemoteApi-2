import 'sim-2'
import 'sim.ZMQRemoteAPI-2'

assert(WORKER_OPTS, 'WORKER_OPTS not defined')

-- CoppeliaSim routes custom function calls (sim.callFunction, script
-- callbacks, UI callbacks, ...) through the global sysCall_ext when it exists;
-- the original function is then not called automatically, so it is re-dispatched
-- here. External calls go to the client by default; Lua is the explicit opt-in:
--
--   'myFunction'       -> Python 'myFunction'
--   'a.b.c.myFunction' -> Python 'a.b.c.myFunction' (resolved through attributes)
--   'myFunction@lua'   -> Lua 'myFunction'
--   'a.b.c.myFunction@lua' -> Lua 'a.b.c.myFunction' (dotted names)
--
-- (A leading '@' is stripped by the engine itself, which then bypasses this
-- function and calls the Lua function directly.)
local function __extCallLua(funcName, ...)
    local f = _G[funcName]
    if type(f) ~= 'function' then
        local obj = _G
        for part in funcName:gmatch('[^.]+') do
            if type(obj) ~= 'table' then
                obj = nil
                break
            end
            obj = obj[part]
        end
        f = obj
    end
    if type(f) ~= 'function' then
        error('no such function: ' .. tostring(funcName))
    end
    return f(...)
end

function sysCall_ext(funcName, ...)
    local luaName = funcName:match('^(.*)@lua$')
    if luaName then
        return __extCallLua(luaName, ...)
    end
    if not rapi then error('the client side is not ready') end
    return rapi:call(nil, funcName, ...)
end


-- true while the external client's sysCall_thread is executing in our coroutine
local clientThreadActive = false

-- set by the client's runOnlyDuringSimulation command: when the simulation is
-- stopped (after having been running at least once since the command was set),
-- this worker stops serving that client and terminates
local runOnlyDuringSimulation = false
local sawSimulationRunning = false

local function isMainThread()
    local co, isMain = coroutine.running()
    return (co == nil) or (isMain == true)
end

local function simulationStopped()
    return sim.scene.simulation.state == sim.simulationState.stopped
end

local function updateSimulationState()
    if not simulationStopped() then
        sawSimulationRunning = true
    end
end

-- true while the client's runOnlyDuringSimulation policy applies, i.e. after the
-- command was given and the simulation ran at least once since then: the worker
-- then stops serving the client (and terminates) as soon as the simulation is
-- stopped.
local function policyActive()
    return runOnlyDuringSimulation and sawSimulationRunning and simulationStopped()
end

local function checkRunOnlyDuringSimulation()
    updateSimulationState()
    if policyActive() then
        error('the simulation stopped (runOnlyDuringSimulation)')
    end
end

function sysCall_init()
    rapi = sim.ZMQRemoteAPI{
        name = 'worker-' .. WORKER_OPTS.port,
        port = WORKER_OPTS.port,
        server = true,
        verbose = WORKER_OPTS.verbose or 0,
        -- The client is an external process: wait on blocking receives and react
        -- as soon as it replies or sends another call, instead of polling with
        -- timeouts.
        blocking = true,
    }
    rapi.callMethod = sim.callMethod
    -- 2 x keepAliveInterval is the silence after which the client is considered
    -- gone (see isRemoteAlive). Raise it via spawnWorker({'keepAliveInterval': N})
    -- if the client's sysCall_thread may compute for a long time without calling
    -- back into the simulation.
    rapi.keepAliveInterval = WORKER_OPTS.keepAliveInterval or 5

    -- Same as in the Python wrapper: a step()/yield() call coming from the
    -- client's thread must be executed inside the thread coroutine (otherwise
    -- it could not suspend it). If it reaches the main thread, defer it.
    rapi.shouldDeferRequest = function(req)
        if not clientThreadActive then return false end
        if not (__coroutine__ and coroutine.status(__coroutine__) ~= 'dead') then return false end
        if req.nested then return false end -- must be answered immediately
        if not isMainThread() then return false end
        return (req.msg == 'call') and ((req.func == 'step') or (req.func == 'yield'))
    end

    -- runOnlyDuringSimulation: once the simulation is stopped, ignore any
    -- remaining incoming command (neither execute nor answer it). The client
    -- is terminated below (bye), so its pending calls end instead of running
    -- outside of a simulation.
    rapi.shouldIgnoreRequest = function()
        return policyActive()
    end

    rapi:log(1, 'spawned worker script (handle=' .. sim.self.handle ..')')

    -- Generic access to CoppeliaSim's Lua modules from the external client
    -- (simUI, simOMPL, simIK, simVision, simAssimp, ...):
    --   rapi.module('simUI').create(xml)
    -- forwards to this callable, which loads the module in this Lua state (the
    -- state that has the C plugins) and calls the function. Returns the Lua
    -- multiple return values, as a list.
    rapi:registerCallback('__module', function(moduleName, funcName, args)
        assert(type(moduleName) == 'string' and type(funcName) == 'string',
               'invalid module call')
        local mod = require(moduleName)
        local fn = mod[funcName]
        assert(type(fn) == 'function',
               'no such function: ' .. moduleName .. '.' .. funcName)
        return table.pack(fn(table.unpack(args or {})))
    end)

    -- Module members are not all functions: e.g. simUI.curve_type.xy is a
    -- plain table of constants, Motion.MoveToPose is a *callable table* (a
    -- middleclass class), and calling it returns a Lua object (an instance) whose
    -- methods must be callable too. __moduleMember tells the client what an
    -- attribute is (function / CBOR-representable value / sub-table / Lua object),
    -- __moduleCall calls a function or callable table at an arbitrary (nested)
    -- path, and __moduleObjectMember/__moduleObjectCall give access to the members
    -- of a Lua object, which is kept in a registry and addressed by an integer id.
    local moduleObjects = {}
    local moduleObjectIds = {} -- reuse the id of an already-registered object
    local moduleObjectCount = 0

    local function isModuleValue(v)
        local t = type(v)
        if (t == 'number') or (t == 'string') or (t == 'boolean') then return true end
        if t ~= 'table' then return false end
        if getmetatable(v) ~= nil then return false end
        for k, val in pairs(v) do
            if ((type(k) ~= 'number') and (type(k) ~= 'string')) or (not isModuleValue(val)) then
                return false
            end
        end
        return true
    end

    -- functions and callable tables (e.g. a middleclass class)
    local function isModuleCallable(v)
        if type(v) == 'function' then return true end
        if type(v) == 'table' then
            local mt = getmetatable(v)
            return mt ~= nil and mt.__call ~= nil
        end
        return false
    end

    -- Lua objects (e.g. a Motion.MoveToPose instance): have a metatable, but are
    -- neither CBOR-encodable (sim objects/vectors carry __tocbor) nor callable
    local function isModuleObject(v)
        local t = type(v)
        if t == 'userdata' then return true end
        if t ~= 'table' then return false end
        local mt = getmetatable(v)
        return mt ~= nil and mt.__tocbor == nil and mt.__call == nil
    end

    local function encodeModuleValue(v)
        if isModuleObject(v) then
            local id = moduleObjectIds[v]
            if id == nil then
                moduleObjectCount = moduleObjectCount + 1
                id = moduleObjectCount
                moduleObjects[id] = v
                moduleObjectIds[v] = id
            end
            return {__luaObject = id}
        end
        return v
    end

    local function decodeModuleValue(v)
        if type(v) == 'table' and v.__luaObject ~= nil then
            return moduleObjects[v.__luaObject]
        end
        return v
    end

    local function decodeModuleArgs(args)
        if type(args) ~= 'table' then return args end
        for i = 1, #args do args[i] = decodeModuleValue(args[i]) end
        return args
    end

    local function packModuleResults(...)
        local n = select('#', ...)
        local res = {}
        for i = 1, n do res[i] = encodeModuleValue(select(i, ...)) end
        res.n = n
        return res
    end

    local function resolveModulePath(moduleName, path)
        local v = require(moduleName)
        for i = 1, #path do v = v[path[i]] end
        return v
    end

    rapi:registerCallback('__moduleMember', function(moduleName, path)
        local v = resolveModulePath(moduleName, path)
        if v == nil then return {kind = 'nil'} end
        if isModuleCallable(v) then return {kind = 'function'} end
        if isModuleObject(v) then
            return {kind = 'object', id = encodeModuleValue(v).__luaObject}
        end
        if isModuleValue(v) then return {kind = 'value', value = v} end
        return {kind = 'table'}
    end)

    rapi:registerCallback('__moduleCall', function(moduleName, path, args)
        local v = resolveModulePath(moduleName, path)
        assert(isModuleCallable(v),
               'not callable: ' .. moduleName .. '.' .. table.concat(path, '.'))
        return packModuleResults(v(table.unpack(decodeModuleArgs(args or {}))))
    end)

    rapi:registerCallback('__moduleObjectMember', function(moduleName, id, name)
        local obj = moduleObjects[id]
        assert(obj ~= nil, 'invalid module object: ' .. tostring(id))
        local v = obj[name]
        if v == nil then return {kind = 'nil'} end
        if isModuleCallable(v) then return {kind = 'function'} end
        if isModuleObject(v) then
            return {kind = 'object', id = encodeModuleValue(v).__luaObject}
        end
        if isModuleValue(v) then return {kind = 'value', value = v} end
        return {kind = 'table'}
    end)

    rapi:registerCallback('__moduleObjectCall', function(moduleName, id, name, args)
        local obj = moduleObjects[id]
        assert(obj ~= nil, 'invalid module object: ' .. tostring(id))
        local fn = obj[name]
        assert(type(fn) == 'function', 'not a method: ' .. tostring(name))
        return packModuleResults(fn(obj, table.unpack(decodeModuleArgs(args or {}))))
    end)

    -- Client command: rapi.call(None, 'runOnlyDuringSimulation') / (..., false).
    -- Once the simulation is stopped, the worker stops serving the client and
    -- terminates (it first sends 'bye', so that the client can exit cleanly).
    rapi:registerCallback('runOnlyDuringSimulation', function(enabled)
        runOnlyDuringSimulation = (enabled == nil) or (not not enabled)
        -- arm it for the next run: only stop once a simulation has actually
        -- been running since the command was set
        sawSimulationRunning = false
        rapi:log(1, 'runOnlyDuringSimulation:', runOnlyDuringSimulation)
        return runOnlyDuringSimulation
    end)

    -- Consulted while a call waits (blocking or yielding): abort it when the
    -- simulation is stopped and the policy is active. The blocking path also
    -- keeps the default liveness check.
    rapi.onWaitTick = checkRunOnlyDuringSimulation
    rapi.onWaitTimeout = function()
        checkRunOnlyDuringSimulation()
        if not rapi:isRemoteAlive() then
            error('the client is not responding anymore')
        end
    end
end

function sysCall_thread()
    local clientThreadStarted = false
    while true do
        updateSimulationState()
        -- If the external client registered a sysCall_thread callback, run it
        -- from within this thread coroutine: it then has exactly the same
        -- preemptive/explicit yielding behaviour as an embedded script's
        -- sysCall_thread (Lua yields, replies may arrive in a later step,
        -- sim.self.step() suspends the client's function, other sysCalls keep
        -- being dispatched to the client meanwhile).
        if (not clientThreadStarted) and rapi._callables['sysCall_thread'] then
            clientThreadStarted = true
            clientThreadActive = true
            local ok, err = xpcall(function()
                rapi:callYielding(nil, 'sysCall_thread')
            end, debug.traceback)
            clientThreadActive = false
            if not ok then
                if policyActive() then
                    rapi:log(1, 'simulation stopped (runOnlyDuringSimulation); terminating worker')
                    break
                end
                if not rapi:isRemoteAlive() then
                    -- The client went away (or stopped responding) while its
                    -- thread was running: terminate this worker instead of
                    -- waiting for a reply that will never arrive.
                    rapi:log(1, 'the client is gone; terminating worker')
                    break
                end
                error(err, 0)
            end
        end
        -- Serve everything that is queued (this is where a client whose main body
        -- drives the simulation is served).
        rapi:spinSome()
        -- Note: at simulation end, commands that arrive while the state is
        -- 'stopped' are ignored by shouldIgnoreRequest (not executed, not
        -- answered): see the hook in sysCall_init. A last call issued while the
        -- simulation is still in its 'lastBeforeStop' state is served normally.
        -- The policy is then checked after serving: the client's last call may
        -- still need its reply, and it should get it before we go away.
        updateSimulationState()
        if policyActive() then
            rapi:log(1, 'simulation stopped (runOnlyDuringSimulation); terminating worker')
            break
        end
        -- Liveness is checked after serving too: keep-alives may have been
        -- queued while the application was halted (popup menu, modal dialog,
        -- edit mode), and that silence is not the client being gone.
        if not rapi:isRemoteAlive() then
            rapi:log(1, 'the client is gone; terminating worker')
            break
        end
        -- Always yield: this loop must let the simulation advance (so that the
        -- registered sysCall callbacks, e.g. sysCall_actuation, keep being served,
        -- and the simulation can progress/stop normally). "Stepping" only concerns
        -- the thread callback (it then yields only on step()/yield()) and the main
        -- body's own step() call -- it must not freeze the simulation.
        sim.self:yield()
    end
    rapi:log(1, 'terminating worker script (handle=' .. sim.self.handle ..')')
    -- Note: the 'bye' is sent from sysCall_cleanup, i.e. *after* the before-hooks
    -- have run. This way the client's registered sysCall_cleanup callback is
    -- still executed while the connection is alive (the client only learns that
    -- we are leaving afterwards).
    sim.self:remove()
end

function sysCall_suspended()
    -- The simulation is paused: the worker's thread coroutine is not resumed,
    -- but this callback still runs. Keep the connection alive (exchange
    -- keep-alives so that neither side considers the other gone during a long
    -- pause) without executing the client's requests: those are queued and only
    -- handled once the simulation runs again, so the client does not keep
    -- working while paused.
    if rapi then
        pcall(function() rapi:spinSuspended() end)
    end
end

function sysCall_addOnScriptSuspend()
    return {cmd = 'cleanup'}
end

function sysCall_cleanup()
    if rapi then
        -- Let the client know we are leaving (best effort).
        pcall(function() rapi:send{msg = 'bye'} end)
        rapi:cleanup()
        rapi = nil
    end
    sim.self:remove()
end

