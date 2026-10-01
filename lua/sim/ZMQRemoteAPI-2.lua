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
local simCBOR = require 'simCBOR'
local simZMQ = require 'simZMQ'
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
    self.time = function() return sim.app.systemTime end

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
    self.__nextId = 0           -- simple incremental ID generator
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
        local id = 'ZMQRemoteAPI'
        if self.name then id = id .. '[' .. self.name .. ']' end
        print(id, ...)
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
--]]
function ZMQRemoteAPI:_sendRequestAndWait(req)
    local req_id = self:_nextId()
    req.id = req_id
    self._pending[req_id] = { done = false, result = nil, error = nil }
    self:send(req)
    self:spinUntilComplete(req_id)
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
--]]
function ZMQRemoteAPI:_sendKeepAlive()
    if self.keepAliveInterval <= 0 then return end
    if self.lastSendTime + self.keepAliveInterval < self.time() then
        self:send {msg = 'noop'}
    end
end

--[[
  Helper: return true if remote is alive (i.e. has sent any message within
  double of the keepAliveInterval).
--]]
function ZMQRemoteAPI:isRemoteAlive()
    if self.keepAliveInterval <= 0 then return true end
    if self.lastRecvTime <= 0 then return true end
    return self.lastRecvTime + 2 * self.keepAliveInterval >= self.time()
end

--[[
  Remote procedure call.
  Sends a 'call' request and waits for the response.
--]]
function ZMQRemoteAPI:call(target, funcName, ...)
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
function ZMQRemoteAPI:registerCallback(funcName, func, global_)
    assert(type(funcName) == 'string', 'invalid function name')
    assert(type(func) == 'function', 'callback must be a function')

    self._callables[funcName] = func

    if not self.server then
        local req = { msg = 'registerCallback', func = funcName, global = not not global_ }
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
    local req_id = req.id

    if msg == 'call' then
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
        self:send{ msg = 'result', id = req_id, error = not ok, result = result }
    elseif msg == 'registerCallback' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            -- Store a wrapper that calls (via zmq) function on the remote side.
            self._callables[req.func] = function(...)
                return self:call(nil, req.func, ...)
            end
            if req.global then
                _G[req.func] = self._callables[req.func]
            end
            return true
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
    else
        self:log(1, 'unsupported message:', msg)
    end
end

--[[
  Processes a single incoming message if one is available.
  Returns true if a message was received and handled, otherwise false.
--]]
function ZMQRemoteAPI:spinOnce()
    self:_sendKeepAlive()
    local msg = self:recv(false)
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
  Blocks and processes incoming messages until the request with the given 'req_id'
  is marked as complete in the pending requests table.
--]]
function ZMQRemoteAPI:spinUntilComplete(req_id)
    while true do
        local pending = self._pending[req_id]
        if pending.done then
            return
        end
        self:spinOnce()
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
  Low‑level send: CBOR‑encodes and sends the message.
  The message must be a table containing at least an `id` and `msg` field.
--]]
function ZMQRemoteAPI:send(msg, block)
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
    local encodeMap = {
        ['function'] = function(f)
            local name = '@tmpcallback_' .. tostring(f)
            if self._callables[name] == nil then
                self._callables[name] = f
            end
            local cbor_c = require 'org.conman.cbor_c'
            return cbor_c.encode(0xC0, 4294999997) .. simCBOR.encode(name)
        end,
    }
    local data = simCBOR.encode(msg, {encodeMap = encodeMap})
    simZMQ.send(self.socket, data, block and 0 or simZMQ.DONTWAIT)
    self.lastSendTime = self.time()
end

--[[
  Low‑level receive: waits for a message and decodes it.
  @param block: if true, blocks indefinitely; if false, uses NOBLOCK.
  Returns the decoded table, or nil if no message is available (non‑block).
--]]
function ZMQRemoteAPI:recv(block)
    assert(self.socket)
    if block then
        self:log(2, 'receiving... (block)')
    end
    local r, data = simZMQ.recv(self.socket, block and 0 or simZMQ.NOBLOCK)
    if r == -1 then return end
    local typeTags = {
        TAG_4294999997 = function(value)
            return function(...)
                return self:call(nil, value, ...)
            end
        end,
    }
    local ok, msg = pcall(simCBOR.decode, data, {typeTags = typeTags})
    if not ok then
        self:log(1, 'invalid CBOR data')
        return
    end
    self:log(2, 'received:', msg)
    self.lastRecvTime = self.time()
    return msg
end

return ZMQRemoteAPI
