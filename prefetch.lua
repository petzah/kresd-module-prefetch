-- very aggressive prefetch
local prefetch = {
    list = {},
    debug = 0,
    persist = false, -- persist restart (e.g. periodically save to file and load on init)
    persist_dumpfile = 'prefetch.dat',
}

function prefetch.init()
    prefetch.ev_trigger = event.after(0, prefetch.refresh)
    prefetch.ev_persist = event.after(60 * sec, prefetch.dump)
    prefetch.load()
end

function prefetch.deinit()
    prefetch.list = {}
end

function prefetch.config(config)
    config = config or {}
    if type(config) ~= 'table' then
        error('[prefetch] configuration must be a table or nil')
    end
    if config.debug then prefetch.debug = config.debug end
    if config.persist then prefetch.persist = config.persist end
    prefetch.deinit()
    prefetch.init()
end

function prefetch.dump()
    if not prefetch.persist then return end
    local f = assert(io.open(prefetch.persist_dumpfile, 'w'), string.format('cannot open "%s" for writing', prefetch.persist_dumpfile))
    for k,v in pairs(prefetch.list) do
        f:write('prefetch.RR{"' .. k .. '",' .. v .. '}')
        f:write('\n')
    end
    f:close()
    prefetch.ev_persist = event.after(60 * sec, prefetch.dump)
end

function prefetch.load()
    if not prefetch.persist then return end
    local f = io.open(prefetch.persist_dumpfile,"r")
    if f == nil then return end
    io.close(f)
    dofile(prefetch.persist_dumpfile)
end

function prefetch.RR(r)
    prefetch.list[tostring(r[1])] = tonumber(r[2])
end

function prefetch.refresh()
    for k, _  in pairs(prefetch.list) do
        if os.time() > prefetch.list[k] then
            local qtype, qname = k:match('^c_(%d+)_(.*)')
            prefetch.list[k] = nil
            if prefetch.debug > 1 then print('refreshing:', qname, qtype) end
            resolve(qname, kres.type[qtype], kres.class.IN)
        end
    end
    prefetch.ev_trigger = event.after(1 * sec, prefetch.refresh)
end

function prefetch.count()
    local cnt = 0
    for _ in pairs(prefetch.list) do
        cnt = cnt + 1
    end
    return cnt
end

prefetch.layer = {
    consume = function (_, req, pkt)
        local qry = req:current()
        if qry.stype ~= kres.type.A then return end
        local records = pkt:section(kres.section.ANSWER)
        if #records < 1 then
            return
        end
        for i=1, #records do
            local rr = records[i]
            if rr.type == kres.type.A or rr.type == kres.type.CNAME then
                local k = 'c_' .. rr.type .. '_' .. kres.dname2str(rr.owner)
                if prefetch.list[k] ~= nil then return end
                prefetch.list[k] = rr.ttl+os.time() -- ttl valid until now()+ttl
            end
        end
    end
}

return prefetch
