--[[
  ZMQRemoteAPI – dual‑role client/server over a single REQ/REP socket.

  Architecture overview:
  -----------------------
  The class can act either as a client (REQ, connects) or as a server (REP, binds).
  Messages are encoded with CBOR and must follow the strict REQ/REP alternation.

  Normal RPC call (client -> server):
    1. client sends {msg='call', func='add', args={1,2}}
    2. server executes add(1,2), replies {msg='result', result={3}}
    3. client receives the result and returns it.

  Client‑registered callback (server -> client):
    1. client sends {msg='registerCallback', func='progress'}
    2. server creates a global function `progress(...)` that calls back to the client.
    3. Later, during a normal call, the server executes `progress(50)`.
    4. The callback wrapper on the server calls `self:call(nil, 'progress', 50)`.
       Because the server is still inside the original `handleRequest` and hasn't sent
       its final reply, this `send` becomes the *reply* to the client's current request.
    5. Client receives {msg='call', func='progress', args={50}}, executes it locally,
       and sends {msg='result', result=...} as its *next request*.
    6. Server receives that result, continues executing the original function,
       and finally sends the ultimate `result` reply to the client.

  This design keeps the REQ/REP alternation intact: every `send` is always a reply
  to the previous `recv`, and vice versa. The nested `self:call` on the server is
  possible because it occupies the “reply” slot of the current transaction.
]]--

local class = require 'middleclass'
local sim = require 'sim-2'
local simCBOR = require 'simCBOR'
local simZMQ = require 'simZMQ'
simZMQ.__raiseErrors()
local uuid = require 'uuid'
uuid.set_rng(uuid.rng.math_random())

local ZMQRemoteAPI = class 'sim.ZMQRemoteAPI'

--[[
  Constructor.
  @param opts table with fields:
    - name      : optional identifier for logging
    - server    : true -> bind as REP server, false -> connect as REQ client
    - verbose   : 0 = quiet, 1 = warnings, 2 = full traffic
    - clientID  : unique id (generated if omitted)
    - host, port: network settings
]]--
function ZMQRemoteAPI:initialize(opts)
    opts = opts or {}
    self.name = opts.name
    self.server = not not opts.server
    self.verbose = tonumber(opts.verbose or 0)
    self.clientID = opts.clientID or uuid.v4()
    local ctx = simZMQ.ctx_singleton()
    local host = opts.host or '127.0.0.1'
    local port = opts.port or 24020
    local socketType = self.server and 'REP' or 'REQ'
    self.socket = simZMQ.socket(ctx, simZMQ[socketType])
    if opts.server then
        simZMQ.bind(self.socket, 'tcp://*:' .. port)
    else
        simZMQ.connect(self.socket, 'tcp://' .. host .. ':' .. port)
    end
end

--[[
  Close and remove the socket.
]]--
function ZMQRemoteAPI:cleanup()
    if self.socket then
        simZMQ.close(self.socket)
        self.socket = nil
    end
end

function ZMQRemoteAPI:__gc()
    self:cleanup()
end

--[[
  Logging helper.
]]--
function ZMQRemoteAPI:log(level, ...)
    if level <= self.verbose then
        local id = 'ZMQRemoteAPI'
        if self.name then id = id .. '[' .. self.name .. ']' end
        print(id, ...)
    end
end

--[[
  XXX: This must be set by user, to wire into appropriate coppeliaSim
  callMethod API. (e.g. it can be wired directly to sim.callMethod, or
  to the callMethod function exposed by a remote API, depending if we
  are in a server or in a client)
]]--
function ZMQRemoteAPI:callMethod(target, methodName, args)
    error 'property "callMethod" is not set'
end

--[[
  Client‑side entry point for a remote procedure call.

  Flow:
    1. Sends a `call` message to the server.
    2. Enters a loop that receives replies.
       - If the reply is a `result` -> unpack and return it.
       - Otherwise (e.g., a `call` from the server, which is a callback invocation)
         -> handle it locally and send back a `result`, then continue waiting
           for the final reply.

  This method is also used by the server’s callback wrapper to invoke a remote
  function on the client. In that context, the `send` acts as the reply to the
  client’s current request, and the subsequent `recv` waits for the client’s
  answer – all while keeping the REQ/REP alternation valid.
]]--
function ZMQRemoteAPI:call(target, funcName, ...)
    assert(type(funcName) == 'string', 'invalid function name type')
    local args = table.pack(...)
    self:send{msg = 'call', target = target, func = funcName, args = args}
    while true do
        local rep = self:recv(true)
        if rep.msg == 'result' then
            if rep.error then
                error(rep.result)
            else
                return table.unpack(rep.result)
            end
        else
            -- The server is asking us to execute a callback (or any other request).
            -- We handle it locally and reply with a `result`, then loop again.
            self:handleRequest(rep)
        end
    end
end

--[[
  Client‑side setup: registers a global function on the server that, when called,
  will transparently invoke the corresponding function on this client.

  The server creates a wrapper in _G[funcName] which does:
      return self:call(nil, funcName, ...)
  so that every call to that global name becomes a remote callback to the client.
]]--
function ZMQRemoteAPI:registerCallback(funcName)
    self:send{msg = 'registerCallback', func = funcName}
    local rep = self:recv(true)
    assert(rep.msg == 'result', 'invalid server reply')
    assert(not rep.error, 'registerCallback failed')
end

--[[
  Handles an incoming request (either a `call` or a `registerCallback`).

  This is the core of both the server and the client request processing.

  `call` branch (server side or client side when receiving a callback):
    - If `target` is provided, it invokes `self:callMethod` (object‑oriented RPC).
    - Otherwise, it looks up the function in _G and calls it with the given args.
    - Important: the called function may contain callbacks that themselves invoke
      `self:call(...)`. Those nested calls will *send a message* (which is the
      reply to the current pending request) and *receive the result* (the next
      request from the other side). This nested loop is perfectly legal because
      each `send`/`recv` pair completes a full REQ/REP transaction.
    - After the function returns, this method sends the final `result` reply.
      (This `result` is the reply to the client’s initial `call` request, or to
      the server’s callback `call` request – depending on who called this handler.)

  `registerCallback` branch (server only):
    - Creates a global wrapper as described in registerCallback().
    - Replies with a `result` to confirm registration.

  Note: `simCBOR.null` is used as a placeholder for Lua `nil` inside result
  tables, because CBOR encoding cannot distinguish `nil` from absent table entries.
  When decoding on the other side (e.g., Python), it becomes a proper `null`.
]]--
function ZMQRemoteAPI:handleRequest(req)
    assert(type(req.msg) == 'string', 'malformed request')
    if req.msg == 'call' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name type: ' .. type(req.func))
            assert(req.target == nil or math.type(req.target) == 'integer', 'invalid target type: ' .. type(req.target))
            local args = req.args or {}
            assert(type(args) == 'table', 'invalid args type: ' .. type(args))
            if req.target then
                -- sim.Object method call (must be implemented by the user of this class)
                return table.pack(self.callMethod(req.target, req.func, table.unpack(args)))
            elseif not req.target then
                local func = _G[req.func]
                assert(func, 'no such function: ' .. req.func)
                assert(type(func) == 'function', 'not a function: ' .. req.func)
                return table.pack(func(table.unpack(args or {})))
            end
        end)

        -- Replace actual `nil` values with `simCBOR.null` so that they survive
        -- CBOR encoding and can be distinguished from missing elements.
        if ok then
            local n = result.n
            result.n = nil
            for i = 1, n do
                if result[i] == nil then
                    result[i] = simCBOR.null
                end
            end
        end
        self:send{msg = 'result', error = not ok, result = result}
    elseif req.msg == 'registerCallback' then
        local ok, result = pcall(function()
            assert(type(req.func) == 'string', 'invalid function name')
            -- When the server calls this global function, it will transparently
            -- perform a remote call back to the client.
            _G[req.func] = function(...)
                return self:call(nil, req.func, ...)
            end
            return true
        end)
        self:send{msg = 'result', error = not ok, result = result}
    else
        self:log(1, 'unsupported message:', req.msg)
    end
end

--[[
  Server‑side main loop: repeatedly receives requests and processes them.
  This should be called only when `self.server` is true.
  The loop continues indefinitely (break only on socket error/closure).
]]--
function ZMQRemoteAPI:handleRequests()
    assert(self.server, 'handleRequests should be called only from server')
    while true do
        local req = self:recv()
        if not req then break end
        self:handleRequest(req)
    end
end

--[[
  Low‑level message sender.
  Encodes the Lua table to CBOR and sends it over the ZMQ socket.
]]--
function ZMQRemoteAPI:send(msg)
    assert(self.socket)
    assert(type(msg) == 'table', 'bad type')
    self:log(2, 'sending:', msg)
    local data = simCBOR.encode(msg)
    simZMQ.send(self.socket, data, 0)
    self:log(2, 'sent')
end

--[[
  Low‑level message receiver.
  @param block: if true, waits indefinitely; if false, uses ZMQ_NOBLOCK.
  Returns the decoded CBOR table, or `nil` if no message was available
  (only when block is false).
]]--
function ZMQRemoteAPI:recv(block)
    assert(self.socket)
    if block then
        self:log(2, 'receiving... (block)')
    end
    local r, data = simZMQ.recv(self.socket, block and 0 or simZMQ.NOBLOCK)
    if r == -1 then return end
    local ok, req = pcall(simCBOR.decode, data)
    if not ok then
        self:log(1, 'invalid request CBOR data')
        return
    end
    self:log(2, 'received:', req)
    return req
end

return ZMQRemoteAPI
