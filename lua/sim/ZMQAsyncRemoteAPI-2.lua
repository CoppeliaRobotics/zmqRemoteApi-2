--[[
  ZMQAsyncRemoteAPI – asynchronous, bidirectional RPC over ZMQ DEALER sockets.

  Architecture overview:
  -----------------------
  This class replaces the REQ/REP‑based ZMQRemoteAPI with a fully asynchronous,
  request‑ID driven design. Both client and server use DEALER sockets, allowing
  either side to send a request (call) at any time, and responses are matched
  by a unique message ID.

  Key features:
    * No strict send/recv alternation – messages can flow in any order.
    * Re‑entrant callbacks: while processing a call, the remote side can invoke
      a callback, and that callback can itself invoke another callback, etc.
    * Both sides can register callbacks (registerCallback) which, when invoked
      on the remote side, will transparently call back to the local side.
    * The `call()` method blocks until its response arrives, but it processes
      any incoming messages (including other requests) while waiting.

  Message protocol:
    Requests:    { id = <unique>, msg = 'call'|'registerCallback',
                   target = <int or nil>, func = <string>, args = <table> }
    Responses:   { id = <same>, msg = 'result', error = <bool>,
                   result = <any> }

  The `id` is a unique identifier (e.g., a UUID or incremental counter) that
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
local simCBOR = require 'simCBOR'
local simZMQ = require 'simZMQ'
simZMQ.__raiseErrors()
local uuid = require 'uuid'
uuid.set_rng(uuid.rng.math_random())

local ZMQAsyncRemoteAPI = class 'sim.ZMQAsyncRemoteAPI'

function ZMQAsyncRemoteAPI:initialize(opts)
    opts = opts or {}
    self.name = opts.name
    self.server = not not opts.server
    self.verbose = tonumber(opts.verbose or 0)
    self.clientID = opts.clientID or uuid.v4()

    local ctx = simZMQ.ctx_singleton()
    local host = opts.host or '127.0.0.1'
    local port = opts.port or 24020

    -- Both sides use DEALER to allow asynchronous bidirectional communication.
    self.socket = simZMQ.socket(ctx, simZMQ.DEALER)
    if self.server then
        simZMQ.bind(self.socket, 'tcp://*:' .. port)
    else
        simZMQ.connect(self.socket, 'tcp://' .. host .. ':' .. port)
    end

    -- Tables for pending requests and local callable functions.
    self._pending = {}          -- id -> { done, result, error }
    self._callables = {}        -- funcName -> function
    self._id_counter = 0        -- simple incremental ID generator
end

function ZMQAsyncRemoteAPI:cleanup()
    if self.socket then
        simZMQ.close(self.socket)
        self.socket = nil
    end
end

function ZMQAsyncRemoteAPI:__gc()
    self:cleanup()
end

function ZMQAsyncRemoteAPI:log(level, ...)
    if level <= self.verbose then
        local id = 'ZMQAsyncRemoteAPI'
        if self.name then id = id .. '[' .. self.name .. ']' end
        print(id, ...)
    end
end

-- XXX: This method must be overridden if object‑oriented calls with `target` are needed.
function ZMQAsyncRemoteAPI:callMethod(target, methodName, args)
    error 'property "callMethod" is not set'
end

--[[
  Generates a unique request ID (simple incrementing counter).
  For production, consider using UUID or a combination with clientID.
--]]
function ZMQAsyncRemoteAPI:_nextId()
    self._id_counter = self._id_counter + 1
    return self._id_counter
end

--[[
  Helper: sends a request and waits for its response.
  While waiting, it processes any incoming messages:
    - If the message is a response (msg='result') for a pending request,
      it updates that pending entry and wakes up the corresponding waiter.
    - If the message is a request (msg='call' or 'registerCallback'),
      it handles it via handleRequest (which may itself send requests and wait).
  This allows re‑entrant callbacks and multiple pending requests.
--]]
function ZMQAsyncRemoteAPI:_sendRequestAndWait(req)
    local id = self:_nextId()
    req.id = id
    self._pending[id] = { done = false, result = nil, error = nil }
    self:send(req)

    while true do
        local pending = self._pending[id]
        if pending.done then
            self._pending[id] = nil
            if pending.error then
                error(pending.result)
            else
                return table.unpack(pending.result)
            end
        end

        local msg = self:recv(true)
        if not msg then
            -- recv should never return nil when blocking; but if it does, break.
            break
        end

        if msg.msg == 'result' then
            local pend = self._pending[msg.id]
            if pend then
                pend.result = msg.result
                pend.error = msg.error
                pend.done = true
            else
                self:log(1, 'received result for unknown id:', msg.id)
            end
        elseif msg.msg == 'call' or msg.msg == 'registerCallback' then
            self:handleRequest(msg)
        else
            self:log(1, 'unexpected message type:', msg.msg)
        end
    end
end

--[[
  Remote procedure call.
  Sends a 'call' request and waits for the response.
  The `target` parameter is unused in this basic implementation (see callMethod).
--]]
function ZMQAsyncRemoteAPI:call(target, funcName, ...)
    assert(type(funcName) == 'string', 'invalid function name type')
    local args = table.pack(...)
    local req = { msg = 'call', target = target, func = funcName, args = args }
    return self:_sendRequestAndWait(req)
end

--[[
  Registers a local function as a callback on the remote side.
  The remote side will store a wrapper that, when called, will invoke this
  local function via a remote 'call' request.
  This method sends a 'registerCallback' request and waits for acknowledgment.
--]]
function ZMQAsyncRemoteAPI:registerCallback(funcName, func)
    assert(type(funcName) == 'string', 'invalid function name')
    assert(type(func) == 'function', 'callback must be a function')
    self._callables[funcName] = func
    local req = { msg = 'registerCallback', func = funcName }
    self:_sendRequestAndWait(req)
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
function ZMQAsyncRemoteAPI:handleRequest(req)
    assert(type(req.msg) == 'string', 'malformed request')
    local id = req.id
    assert(id ~= nil, 'request missing id')

    if req.msg == 'call' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            local args = req.args or {}
            assert(type(args) == 'table', 'invalid args type')

            if req.target then
                -- Object‑oriented call – must be implemented by a subclass.
                return table.pack(self.callMethod(req.target, req.func, table.unpack(args)))
            else
                local func = self._callables[req.func]
                assert(func, 'no such function: ' .. req.func)
                assert(type(func) == 'function', 'not a function: ' .. req.func)
                return table.pack(func(table.unpack(args)))
            end
        end)

        -- Replace actual nil values with simCBOR.null for proper CBOR encoding.
        if ok then
            local n = result.n
            result.n = nil
            for i = 1, n do
                if result[i] == nil then
                    result[i] = simCBOR.null
                end
            end
        end

        self:send{ msg = 'result', id = id, error = not ok, result = result }

    elseif req.msg == 'registerCallback' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            -- Store a wrapper that calls back to the remote side.
            --[[ (this won't work properly)
            self._callables[req.func] = function(...)
                -- This will send a 'call' request to the other side.
                return self:call(nil, req.func, ...)
            end
            ]]--
            _G[req.func] = function(...)
                return self:call(nil, req.func, ...)
            end
            print('DEBUG: registered a callback "' .. req.func .. '". current global functions: ' .. table.join(filter(function(k) return type(_G[k]) == 'function' end, table.keys(_G)), ', '))
            return true
        end)
        self:send{ msg = 'result', id = id, error = not ok, result = result }

    else
        self:log(1, 'unsupported message:', req.msg)
    end
end

--[[
  Main loop for processing incoming requests.
  This can be called on either side, but is typically used on the server
  to continuously handle incoming calls and registrations.
  It receives any message:
    - If it is a request (call/registerCallback), it handles it.
    - If it is a result, it updates the pending table (for any outstanding
      request that may have been sent by this side). If no pending request
      matches, the result is logged and ignored.
  The loop runs indefinitely until an error occurs or the socket is closed.
--]]
function ZMQAsyncRemoteAPI:handleRequests()
    while true do
        local msg = self:recv(true)
        if not msg then break end

        if msg.msg == 'result' then
            local pend = self._pending[msg.id]
            if pend then
                pend.result = msg.result
                pend.error = msg.error
                pend.done = true
            else
                self:log(1, 'received result for unknown id:', msg.id)
            end
        elseif msg.msg == 'call' or msg.msg == 'registerCallback' then
            self:handleRequest(msg)
        else
            self:log(1, 'unexpected message type:', msg.msg)
        end
    end
end

-- Poll for one message with a timeout (in milliseconds).
-- Returns true if a message was processed, false otherwise.
function ZMQAsyncRemoteAPI:poll(timeoutMs)
    timeoutMs = timeoutMs or 0
    -- Use simZMQ.poll if available, or fallback to non-blocking recv with sleep
    local r, data
    if simZMQ.poll then
        -- If simZMQ provides poll, use it
        local events = simZMQ.poll(self.socket, timeoutMs)
        if events == 0 then return false end
        r, data = simZMQ.recv(self.socket, 0)  -- non-blocking, but we know data is ready
    else
        -- Fallback: use non-blocking recv in a loop with small sleeps
        local start = simZMQ.getTimeInMs()
        while true do
            r, data = simZMQ.recv(self.socket, simZMQ.NOBLOCK)
            if r ~= -1 then break end
            if simZMQ.getTimeInMs() - start >= timeoutMs then return false end
            simZMQ.sleep(0.001)  -- yield to avoid busy-wait
        end
    end
    if r == -1 then return false end
    local ok, msg = pcall(simCBOR.decode, data)
    if not ok then
        self:log(1, 'invalid CBOR data')
        return false
    end
    self:log(2, 'received:', msg)
    self:_processMessage(msg)
    return true
end

-- Internal: process a single decoded message (result or request)
function ZMQAsyncRemoteAPI:_processMessage(msg)
    if msg.msg == 'result' then
        local pend = self._pending[msg.id]
        if pend then
            pend.result = msg.result
            pend.error = msg.error
            pend.done = true
        else
            self:log(1, 'received result for unknown id:', msg.id)
        end
    elseif msg.msg == 'call' or msg.msg == 'registerCallback' then
        self:handleRequest(msg)
    else
        self:log(1, 'unexpected message type:', msg.msg)
    end
end

-- Process messages repeatedly until timeoutMs expires (or forever if < 0)
function ZMQAsyncRemoteAPI:processRequests(timeoutMs)
    timeoutMs = timeoutMs or -1
    if timeoutMs < 0 then
        while true do
            self:poll(100)  -- poll with small chunk to keep responsive
        end
    else
        local start = simZMQ.getTimeInMs()
        while simZMQ.getTimeInMs() - start < timeoutMs do
            self:poll(math.min(timeoutMs - (simZMQ.getTimeInMs() - start), 100))
        end
    end
end

--[[
  Low‑level send: CBOR‑encodes and sends the message.
  The message must be a table containing at least an `id` and `msg` field.
--]]
function ZMQAsyncRemoteAPI:send(msg)
    assert(self.socket)
    assert(type(msg) == 'table', 'bad type')
    self:log(2, 'sending:', msg)

    -- XXX: fix packed tables by replacing nils with simCBOR.null
    local nfixed = 0
    for _, key in ipairs{'args', 'result'} do
        local tbl = msg[key]
        if type(tbl) == 'table' and math.type(tbl.n) == 'integer' then
            self:log(2, '    key "' .. key .. '" contains a packed table')
            local newtbl = {}
            for i = 1, tbl.n do
                if tbl[i] == nil then
                    newtbl[i] = simCBOR.null
                    self:log(2, '    fixed nil element ' .. i .. ' of packed table')
                else
                    newtbl[i] = tbl[i]
                end
            end
            nfixed = nfixed + 1
            msg[key] = newtbl
        end
    end
    if nfixed > 0 then
        self:log(2, 'sending (fixed ' .. nfixed .. ' keys):', msg)
    end

    local data = simCBOR.encode(msg)
    simZMQ.send(self.socket, data, 0)
    self:log(2, 'sent')
end

--[[
  Low‑level receive: waits for a message and decodes it.
  @param block: if true, blocks indefinitely; if false, uses NOBLOCK.
  Returns the decoded table, or nil if no message is available (non‑block).
--]]
function ZMQAsyncRemoteAPI:recv(block)
    assert(self.socket)
    if block then
        self:log(2, 'receiving... (block)')
    end
    local r, data = simZMQ.recv(self.socket, block and 0 or simZMQ.NOBLOCK)
    if r == -1 then return end

    local ok, msg = pcall(simCBOR.decode, data)
    if not ok then
        self:log(1, 'invalid CBOR data')
        return
    end
    self:log(2, 'received:', msg)
    return msg
end

return ZMQAsyncRemoteAPI
