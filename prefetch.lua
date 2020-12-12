-- very aggressive prefetch
local prefetch = {
    list = {},
    debug = 0,
}

function prefetch.init()
    prefetch.ev_trigger = event.after(0, prefetch.refresh)
end

function prefetch.deinit()
    prefetch.list = {}
end

function prefetch.refresh()
    for k, _  in pairs(prefetch.list) do
        if os.time() > prefetch.list[k] then
            local qtype, qname = k:match('^c_(%d+)_(.*)')
            prefetch.list[k] = nil
            if prefetch.debug > 0 then print('refreshing:', qname, qtype) end
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
        local records = pkt:section(kres.section.ANSWER)
        if #records < 1 then
            return
        end
        for i=1, #records do
            local rr = records[i]
            if rr.type == kres.type.A or rr.type == kres.type.CNAME then
                local k = 'c_' .. rr.type .. '_' .. kres.dname2str(rr.owner)
                if prefetch[k] == nil then
                    prefetch.list[k] = rr.ttl+os.time() -- ttl valid until now()+ttl
                end
            end
        end
    end
}

return prefetch
