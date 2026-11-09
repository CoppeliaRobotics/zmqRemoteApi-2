--[[
  ZMQRemoteAPI – asynchronous, bidirectional RPC over ZMQ DEALER sockets.

  Architecture overview:
  -----------------------
  Fully asynchronous, request‑ID driven design. Both client and server use
  DEALER sockets, allowing either side to send a request (call) at any time,
  and responses are matched by a unique message ID.

  Key features:
    * No strict send/recv alternation – messages can flow in any order.
    * Re‑entrant callbacks: while processing a call, the remote side can invoke
      a callback, and that callback can itself invoke another callback, etc.
    * Both sides can register callbacks (registerCallback) which, when invoked
      on the remote side, will transparently call back to the local side.
    * The `call()` method blocks until its response arrives, but it processes
      any incoming messages (including other requests) while waiting.

  Message protocol:
    Requests:    { id = <unique>, msg = 'call',
                   target = <int or nil>, func = <string>, args = <table> }

                 { id = <unique>, msg = 'registerCallback',
                   func = <string> }

                 { msg = 'noop' }

    Responses:   { id = <same>, msg = 'result', error = <bool>,
                   result = <any> }

  The `id` is a unique identifier (e.g.: incremental counter) that
  allows the receiver to match a response to the original request.

  Socket setup:
    * Server (self.server = true)  -> binds a DEALER socket.
    * Client (self.server = false) -> connects a DEALER socket.
    Only a single client is supported (the DEALER socket is connected to one peer).

  Function lookup:
    Both sides maintain a table `self._callables` that maps function names to
    Lua functions. When a 'call' request arrives, the function is looked up in
    this table and executed. A 'registerCallback' request installs a wrapper
    that, when called, will send a remote 'call' to the other side.
--]]
local class = require 'middleclass'
local sim = require 'sim-2'
local sim_cbor = require 'sim.cbor'
local simZMQ = require 'simZMQ'
local array = require 'array'
simZMQ.__raiseErrors()

local ZMQRemoteAPI = class 'sim.ZMQRemoteAPI'

function ZMQRemoteAPI:initialize(opts)
    opts = opts or {}
    self.name = opts.name
    self.server = not not opts.server
    self.verbose = tonumber(opts.verbose or 0)
    self.lastSendTime = 0
    self.lastRecvTime = 0
    self.keepAliveInterval = 5
    -- Overrides the "remote is gone" silence tolerance: nil = 2 x
    -- keepAliveInterval, 0 = never (see isRemoteAlive).
    self.remoteSilenceTimeout = nil
    self.time = function() return sim.app.systemTime end
    self.print = print

    -- When blocking is set, a call waits on a blocking zmq_poll and reacts as
    -- soon as a message arrives (no timeout granularity). Used by the Python
    -- wrapper. When false (default), waits stay preemptable: they poll in small
    -- slices so that the Lua thread can yield (used e.g. by the worker script).
    self.blocking = not not opts.blocking

    local ctx = simZMQ.ctx_singleton()
    local host = opts.host or '127.0.0.1'
    local port = opts.port or 24020

    -- Both sides use DEALER to allow asynchronous bidirectional communication.
    self.socket = simZMQ.socket(ctx, simZMQ.DEALER)
    if self.server then
        -- Do not ignore a failed bind: an unbound server would leave every
        -- client waiting forever (the failure is otherwise silent, since
        -- error raising is disabled on the simZMQ wrappers).
        local r = simZMQ.bind(self.socket, 'tcp://*:' .. port)
        if r ~= 0 then
            error('could not bind to port ' .. port)
        end
    else
        local r = simZMQ.connect(self.socket, 'tcp://' .. host .. ':' .. port)
        if r ~= 0 then
            error('could not connect to ' .. host .. ':' .. port)
        end
    end

    -- Tables for pending requests and local callable functions.
    self._pending = {}          -- id -> { done, result, error }
    self._callables = {}        -- funcName -> function
    self.__nextId = 0           -- simple incremental ID generator

    -- Requests that could not be handled in the current context and were
    -- deferred to a later spin (see `shouldDeferRequest` below).
    self._deferred = {}
    self.shouldDeferRequest = nil -- optional function(req) -> bool

    -- Built once (instead of for each message) to avoid per-message garbage:
    self._encodeOpts = {
        encodeMap = {
            ['function'] = function(f)
                local name = '@tmpcallback_' .. string.functostring(f)
                if self._callables[name] == nil then
                    self._callables[name] = f
                end
                local cbor_c = require 'org.conman.cbor_c'
                return cbor_c.encode(0xC0, 4294999997) .. sim_cbor.encode(name)
            end,
        },
    }
    self._decodeOpts = {
        typeTags = {
            TAG_4294999997 = function(value)
                return function(...)
                    return self:call(nil, value, ...)
                end
            end,
        },
    }
    self._keepAliveCnt = 0

    -- Built-in diagnostic: lets a client check that this side is alive and
    -- serving. It can be called with a timeout, e.g.
    -- rapi.callEx(None, '__ping', (), timeout=5.0).
    self._callables['__ping'] = function() return 'pong' end
end

function ZMQRemoteAPI:cleanup()
    if self.socket then
        simZMQ.close(self.socket)
        self.socket = nil
    end
end

function ZMQRemoteAPI:__gc()
    self:cleanup()
end

function ZMQRemoteAPI:log(level, ...)
    if level <= self.verbose then
        local msg = 'ZMQRemoteAPI'
        if self.name then msg = msg .. '[' .. self.name .. ']' end
        local args = table.pack(...)
        for i = 1, args.n do
            msg = msg .. ' ' .. string.anytostring(args[i], {omitQuotes = true})
        end
        self.print(msg)
    end
end

-- XXX: This method must be overridden if object‑oriented calls with `target` are needed.
function ZMQRemoteAPI:callMethod(target, methodName, args)
    error 'property "callMethod" is not set'
end

--[[
  Generates a unique request ID (simple incrementing counter).
  For production, consider using UUID or a combination with clientID.
--]]
function ZMQRemoteAPI:_nextId()
    self.__nextId = self.__nextId + 1
    return self.__nextId
end

--[[
  Helper: sends a request and waits for its response.
  When `yielding` is true, the wait remains preemptable (it polls in small
  slices so that the Lua thread can yield and the simulation can advance).
  Use that for calls that may take an arbitrarily long time to be answered,
  e.g. a Python sysCall_thread: what is being waited for is not a simple
  reply, but a whole Python thread execution.
--]]
function ZMQRemoteAPI:_sendRequestAndWait(req, yielding)
    local req_id = self:_nextId()
    req.id = req_id
    self._pending[req_id] = { done = false, result = nil, error = nil }
    self:send(req)
    self:spinUntilComplete(req_id, yielding)
    local pending = self._pending[req_id]
    assert(pending and pending.done)
    self._pending[req_id] = nil
    if pending.error then
        error(pending.result)
    else
        return table.unpack(pending.result)
    end
end

--[[
  Helper: sends a keep-alive message (noop) if needed.
  The system time is relatively expensive to read, and the keep-alive interval
  is several seconds, so it is only checked every now and then.
--]]
function ZMQRemoteAPI:_sendKeepAlive()
    if self.keepAliveInterval <= 0 then return end
    self._keepAliveCnt = self._keepAliveCnt + 1
    if self._keepAliveCnt < 16 then return end
    self._keepAliveCnt = 0
    if self.lastSendTime + self.keepAliveInterval < self.time() then
        self:send {msg = 'noop'}
    end
end

--[[
  Helper: return true if remote is alive (i.e. has sent any message within
  double of the keepAliveInterval). A 'bye' message sent by the other side
  (see Python ZMQRemoteAPI.cleanup) makes this return false immediately.

  `remoteSilenceTimeout` overrides the silence tolerance (0 disables it, for
  owners that detect the other side's death by other means, e.g. the embedded
  Python wrapper, which polls its subprocess).
--]]
function ZMQRemoteAPI:isRemoteAlive()
    if self._remoteBye then return false end
    if self.keepAliveInterval <= 0 then return true end
    if self.lastRecvTime <= 0 then return true end
    local timeout = self.remoteSilenceTimeout
    if timeout == nil then timeout = 2 * self.keepAliveInterval end
    if timeout <= 0 then return true end
    return self.lastRecvTime + timeout >= self.time()
end

--[[
  Remote procedure call.
  Sends a 'call' request and waits for the response.
--]]
function ZMQRemoteAPI:call(target, funcName, ...)
    assert(type(funcName) == 'string', 'invalid function name type')
    local args = array:pack(...)
    local req = { msg = 'call', target = target, func = funcName, args = args }
    return self:_sendRequestAndWait(req)
end

--[[
  Same as call(), but the wait stays preemptable: in a Lua thread the call may
  be suspended (yield) while waiting, so that the simulation keeps running.
  Meant for calls that can take a long time to be answered (e.g. a Python
  sysCall_thread, which only returns when the Python thread function returns).
--]]
function ZMQRemoteAPI:callYielding(target, funcName, ...)
    assert(type(funcName) == 'string', 'invalid function name type')
    local args = array:pack(...)
    local req = { msg = 'call', target = target, func = funcName, args = args }
    return self:_sendRequestAndWait(req, true)
end

--[[
  Registers a local function as a callback on the remote side.
  The remote side will store a wrapper that, when called, will invoke this
  local function via a remote 'call' request.
  This method sends a 'registerCallback' request and waits for acknowledgment.
--]]
function ZMQRemoteAPI:registerCallback(funcName, func, sysCall)
    assert(type(funcName) == 'string', 'invalid function name')
    assert(type(func) == 'function', 'callback must be a function')

    self._callables[funcName] = func

    if not self.server then
        local req = { msg = 'registerCallback', func = funcName, sysCall = not not sysCall }
        self:_sendRequestAndWait(req)
    end
end

--[[
  Handles an incoming request (call or registerCallback).
  This is the core dispatcher on both client and server.

  For 'call':
    - Looks up the function in self._callables.
    - If `target` is given, uses self.callMethod (must be overridden).
    - Executes the function, catches errors, and sends a 'result' response
      with the same `id` as the request.

  For 'registerCallback':
    - Installs a wrapper in self._callables that, when called, will send a
      remote 'call' to the other side (effectively invoking the remote function).
    - Replies with a 'result' to confirm.

  Note: The request is expected to have an `id` field, which is used in the response.
--]]
function ZMQRemoteAPI:handleRequest(req)
    local msg = req.msg
    assert(type(msg) == 'string', 'malformed request')

    -- The owner may decide that requests must be ignored altogether (e.g. a
    -- simulation-only worker once the simulation has been stopped): they are
    -- neither executed nor answered.
    if (msg == 'call' or msg == 'registerCallback') and self.shouldIgnoreRequest
        and self.shouldIgnoreRequest(req) then
        self:log(2, 'ignoring request:', msg, req.func)
        return
    end

    -- The owner may decide that a request cannot be handled in the current
    -- context (e.g. because it has to run in a specific Lua coroutine). Such
    -- a request is only answered once it is picked up from the deferred queue
    -- in a context where it can be handled.
    if self.shouldDeferRequest and self.shouldDeferRequest(req) then
        self._deferred[#self._deferred + 1] = req
        self:log(2, 'deferred request:', msg, req.func)
        return
    end

    local req_id = req.id

    if msg == 'call' then
        local ok, result = xpcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            local args = req.args or {}
            assert(type(args) == 'table', 'invalid args type')

            if req.target then
                -- Object‑oriented call – must be implemented by a subclass.
                return array:pack(self.callMethod(req.target, req.func, table.unpack(args)))
            else
                local func = self._callables[req.func]
                assert(func, 'no such function: ' .. req.func)
                assert(type(func) == 'function', 'not a function: ' .. req.func)
                return array:pack(func(table.unpack(args)))
            end
        end, debug.traceback)
        self:send{ msg = 'result', id = req_id, error = not ok, result = result }
    elseif msg == 'registerCallback' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            -- Store a wrapper that calls (via zmq) function on the remote side.
            self._callables[req.func] = function(...)
                return self:call(nil, req.func, ...)
            end
            if req.sysCall then
                -- sysCall_thread is special: it is not a regular callback (it is
                -- driven by the owner, i.e. the worker script or the Python
                -- wrapper, from a thread coroutine). Register a function hook
                -- for everything else. Cleanup is a "before" hook so that the
                -- remote side is still connected when it runs.
                if req.func ~= 'sysCall_thread' then
                    local when = 'after'
                    if req.func == 'sysCall_cleanup' then when = 'before' end
                    local cb = self._callables[req.func]
                    if req.func == 'sysCall_cleanup' then
                        -- This script may be cleaned up long after the remote
                        -- side went away (e.g. the client process was killed).
                        -- Calling into it then would block until the liveness
                        -- timeout and raise from within sysCall_cleanup. Also
                        -- never let a failing cleanup callback raise: a cleanup
                        -- is not the place to report such an error.
                        local cleanupCb = cb
                        cb = function(...)
                            if not self:isRemoteAlive() then
                                self:log(2, 'not calling the remote cleanup callback: remote is gone')
                                return
                            end
                            local ok2, err2 = pcall(cleanupCb, ...)
                            if not ok2 then
                                self:log(1, 'remote cleanup callback failed:', err2)
                            end
                        end
                    end
                    sim.self:registerFunctionHook(req.func, cb, {when = when})
                end
            end
            return {true} -- result must be packed into an array!
        end)
        self:send{ msg = 'result', id = req_id, error = not ok, result = result }
    elseif msg == 'result' then
        local pend = self._pending[req_id]
        if pend then
            pend.result = req.result
            pend.error = req.error
            pend.done = true
        else
            self:log(1, 'received result for unknown id:', req_id)
        end
    elseif msg == 'noop' then
    elseif msg == 'bye' then
        -- The other side is leaving: no reply is sent, but this side can stop
        -- waiting for messages that will never come.
        self._remoteBye = true
    else
        self:log(1, 'unsupported message:', msg)
    end
end

--[[
  Processes a single incoming message if one is available (or, with `block`,
  waits until one arrives).
  Returns true if a message was received and handled, otherwise false.
  Deferred requests are handled first, as soon as the context allows it.
--]]
function ZMQRemoteAPI:spinOnce(block)
    self:_sendKeepAlive()
    if #self._deferred > 0 then
        local req = self._deferred[1]
        if not (self.shouldDeferRequest and self.shouldDeferRequest(req)) then
            table.remove(self._deferred, 1)
            self:handleRequest(req)
            return true
        end
    end
    local msg = self:recv(block)
    if msg then
        self:handleRequest(msg)
        return true
    else
        return false
    end
end

--[[
  Processes incoming messages for up to 'timeout' seconds (default: indefinitely).
  Continues processing as long as messages are available, but stops early if no message
  arrives within a short poll interval (max 0.1s).
--]]
function ZMQRemoteAPI:spinSome(timeout)
    if timeout then
        local start = self.time()
        while true do
            local remaining = timeout - (self.time() - start)
            if remaining <= 0 then return end
            self:_sendKeepAlive()
            if self:poll(math.min(remaining, 0.1)) then
                self:spinOnce()
            end
        end
    else
        while self:spinOnce() do end
    end
end

--[[
  Serves the connection while the simulation is paused, without executing
  incoming requests.

  Keep-alives are exchanged (so neither side considers the other gone) and
  replies to our own pending requests are processed, but 'call' and
  'registerCallback' requests are queued instead of being executed: they are
  only handled by spinOnce() once the simulation runs again. That way a client
  keeps running only while the simulation is running, and simply resumes where
  it left off (it stays blocked on its pending call meanwhile).
--]]
function ZMQRemoteAPI:spinSuspended()
    if self._remoteBye then return end
    self:_sendKeepAlive()
    while true do
        local msg = self:recv(false)
        if msg == nil then return end
        if msg.msg == 'call' or msg.msg == 'registerCallback' then
            self._deferred[#self._deferred + 1] = msg
        else
            self:handleRequest(msg) -- noop, result, bye
        end
    end
end

--[[
  Blocks and processes incoming messages until the request with the given 'req_id'
  is marked as complete in the pending requests table.

  With a blocking API instance, this waits on a blocking zmq_poll: the call
  returns as soon as any message arrives (a reply, but also a re-entrant call
  from the other side), with no timeout granularity involved.

  With a yielding=... call (see callYielding), the wait stays preemptable: it
  receives without blocking and polls in 1ms slices, so that the Lua thread can
  yield and the simulation keeps running while e.g. Python executes the thread.
--]]
function ZMQRemoteAPI:spinUntilComplete(req_id, yielding)
    local blocking = self.blocking and not yielding
    local stall = 0
    while true do
        local pending = self._pending[req_id]
        if pending.done then
            return
        end
        if blocking then
            self:spinOnce(true)
        else
            if not self:spinOnce(false) then
                if yielding then
                    -- A thread that waits for the other side must not wait
                    -- forever if that side disappeared (e.g. the external client
                    -- process was closed while its sysCall_thread was running).
                    -- Check about twice per second; the check itself is only
                    -- done then (it reads a property).
                    stall = stall + 1
                    if stall >= 500 then
                        stall = 0
                        -- Owner hook: may abort the wait (e.g. the worker's
                        -- runOnlyDuringSimulation policy once the simulation is
                        -- stopped).
                        if self.onWaitTick then
                            local ok2, err2 = pcall(self.onWaitTick)
                            if not ok2 then
                                self._pending[req_id] = nil
                                error(err2, 0)
                            end
                        end
                        if not self:isRemoteAlive() then
                            self._pending[req_id] = nil
                            error('the remote side is not responding anymore')
                        end
                    end
                end
                self:poll(0.001)
            else
                stall = 0
            end
        end
    end
end

--[[
  Low-level poll: poll for one incoming message with a timeout (in seconds).
  Returns true if a message is available, false otherwise.
--]]
function ZMQRemoteAPI:poll(timeout)
    assert(self.socket)
    timeout = timeout or 0
    return simZMQ.poll({self.socket}, {simZMQ.POLLIN}, math.floor(timeout * 1000)) > 0
end

--[[
  Blocks until a message is available. This is a true blocking zmq_poll: it
  returns as soon as a message arrives, i.e. without any polling granularity.
  A (long) timeout is used only to periodically give the owner a chance to
  check that the remote side is still alive (see `onWaitTimeout`).
--]]
function ZMQRemoteAPI:waitForData()
    assert(self.socket)
    local failures = 0
    while true do
        local n = simZMQ.poll({self.socket}, {simZMQ.POLLIN}, 500)
        if n > 0 then
            return
        elseif n < 0 then
            -- Usually an interruption (EINTR): retry a few times.
            failures = failures + 1
            if failures >= 10 then
                error('ZMQ poll failed: ' .. tostring(simZMQ.strerror(simZMQ.errnum())))
            end
        else
            failures = 0
            -- Keep the connection alive while we wait: the other side applies
            -- the same liveness timeout to us, so a long wait (e.g. while it
            -- computes, or while the simulation is paused) must not look dead.
            self:_sendKeepAlive()
            if self.onWaitTimeout then
                self.onWaitTimeout() -- may raise (e.g. remote process died)
            elseif not self:isRemoteAlive() then
                -- Keep the stock behaviour for owners that don't override
                -- onWaitTimeout (e.g. the worker script): do not block forever
                -- when the other side is gone.
                error('the remote side is not responding anymore')
            end
        end
    end
end

--[[
  Low‑level send: CBOR‑encodes and sends the message.
  The message must be a table containing at least an `id` and `msg` field.
--]]
function ZMQRemoteAPI:send(msg, block)
    assert(self.socket)
    assert(type(msg) == 'table', 'bad type')
    self:log(2, 'sending:', msg)
    local data = sim_cbor.encode(msg, self._encodeOpts)
    simZMQ.send(self.socket, data, block and 0 or simZMQ.DONTWAIT)
    self.lastSendTime = self.time()
end

--[[
  Low‑level receive: waits for a message and decodes it.
  @param block: if true, waits on a blocking poll; if false, uses NOBLOCK.
  Returns the decoded table, or nil if no message is available (non‑block).
--]]
function ZMQRemoteAPI:recv(block)
    assert(self.socket)
    if block then
        self:log(2, 'receiving... (block)')
        self:waitForData()
    end
    local r, data = simZMQ.recv(self.socket, simZMQ.NOBLOCK)
    if r == -1 then return end
    local ok, msg = pcall(sim_cbor.decode, data, self._decodeOpts)
    if not ok then
        self:log(1, 'invalid CBOR data')
        return
    end
    self:log(2, 'received:', msg)
    self.lastRecvTime = self.time()
    return msg
end

return ZMQRemoteAPI
