
local tracker_host = "192.168.85.249"
local tracker_port = 22122

local tracker_keepalive = 100
local tracker_keepalive_timeout = 8 * 3600 * 1000
local tracker_timeout = 1000
local storage_keepalive = 100
local storage_timeout = 1000
local storage_keepalive_timeout = 8 * 3600 * 1000

local FDFS_PROTO_PKG_LEN_SIZE = 8
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE = 104
local TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103
local STORAGE_PROTO_CMD_UPLOAD_FILE = 11
local STORAGE_PROTO_CMD_DELETE_FILE = 12
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_PROTO_CMD_QUIT = 82
local TRACKER_PROTO_CMD_RESP = 100

function int2buf(n)
    -- only trans 32bit  full is 64bit
    local out = {}
    out[#out+1] = string.rep("\00",4)
    out[#out+1] = string.char(bit.band(bit.rshift(n, 24), 0xff))
    out[#out+1] = string.char(bit.band(bit.rshift(n, 16), 0xff))
    out[#out+1] = string.char(bit.band(bit.rshift(n, 8), 0xff))
    out[#out+1] = string.char(bit.band(n, 0xff))
    return table.concat(out)
end

function buf2int(buf)
    -- only trans 32bit  full is 64bit
    return string.byte(buf,8) + string.byte(buf,7) * 256 + string.byte(buf,6) * 65536 +  string.byte(buf,4) * 16777216
end

function read_fdfs_header(sock)
    local header = {}
    local buf = sock:receive(10)
    header.len = buf2int(string.sub(buf, 1, 8))
    header.cmd = string.byte(buf, 9)
    header.status = string.byte(buf, 10)
    return header
end

function fix_string(str, fix_length)
    local len = string.len(str)
    if len > fix_length then
        len = fix_length
    end
    local fix_str = string.sub(str, 1, len)
    if len < fix_length then
        fix_str = fix_str .. string.rep("\00", fix_length - len )
    end
    return fix_str
end

function strip_string(str)
    local pos = str:find("\00")
    if pos then
        return string.sub(str, 1, pos - 1)
    else
        return str
    end
end

function get_ext_name(filename)
    local extname = filename:match("%.(%w+)$")
    if extname then
        return fix_string(extname, FDFS_FILE_EXT_NAME_MAX_LEN)
    else
        return nil
    end
end

function read_tracket_result(sock, header)
    if header.len > 0 then
        local res = {}
        local buf = sock:receive(header.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.host       = strip_string(string.sub(buf, 17, 31)) 
        res.port       = buf2int(string.sub(buf, 32, 39))
        res.store_path_index = string.byte(string.sub(buf, 40, 40))
        return res
    else
        return nil
    end
end

function query_upload_storage(group_name)
    local out = {}
    if group_name then
        -- query upload with group_name
        -- package length
        out[#out+1] = int2buf(16)
        -- cmd
        out[#out+1] = string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE)
        -- status
        out[#out+1] = "\00"
        -- group name
        out[#out+1] = fix_string(group_name, 16)
    else
        -- query upload without group_name
        -- package length
        out[#out+1] = string.rep("\00", FDFS_PROTO_PKG_LEN_SIZE)
        -- cmd
        out[#out+1] = string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE)
        -- status
        out[#out+1] = "\00"
    end
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(tracker_timeout)
    -- connect tracker
    local ok, err = sock:connect(tracker_host, tracker_port)
    if not ok then
        return nil, err
    end
    -- send request
    local bytes, err = sock:send(table.concat(out))
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read body
    local res = read_tracket_result(sock, hdr)
    -- keepalive
    sock:setkeepalive(tracker_keepalive_timeout, tracker_keepalive)
    return res
end

function read_storage_result(sock, header)
    if header.len > 0 then
        local res = {}
        local buf = sock:receive(header.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.file_name  = strip_string(string.sub(buf, 17, header.len))
        return res
    else
        return nil
    end
end

function do_upload_storage(storage, ext_name)
    -- ext_name
    if ext_name then
        ext_name = fix_string(ext_name, FDFS_FILE_EXT_NAME_MAX_LEN)
    end
    -- get file size
    local file_size = tonumber(ngx.var.content_length)
    if not file_size or file_size <= 0 then
        return nil
    end
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(storage_timeout)
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    -- send header
    hdr = {}
    hdr[#hdr+1] = int2buf(file_size + 15)
    hdr[#hdr+1] = string.char(STORAGE_PROTO_CMD_UPLOAD_FILE)
    -- status
    hdr[#hdr+1] = "\00"
    -- store_path_index
    hdr[#hdr+1] = string.char(storage.store_path_index)
    -- filesize
    hdr[#hdr+1] = int2buf(file_size)
    -- exitname
    hdr[#hdr+1] = ext_name
    local bytes, err = sock:send(table.concat(hdr))
    -- send file data
    local send_count = 0
    local req_sock, err = ngx.req.socket()
    if not req_sock then
        ngx.log(ngx.ERR,err)
        ngx.exit(500)
    end
    while true do
        local chunk, _, part = req_sock:receive(1024 * 64)
        if not part then
            local bytes, err = sock:send(chunk)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "send body error")
                sock:close()
                ngx.exit(500)
            end
        else
            -- part have data, not read full end
            local bytes, err = sock:send(part)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "send body error")
                sock:close()
                ngx.exit(500)
            end
            break
        end
    end
    -- read response
    local res_hdr = read_fdfs_header(sock)
    local res = read_storage_result(sock, res_hdr)
    sock:setkeepalive(3600*1000, storage_keepalive)
    return res
end

function query_delete_storage_ex(group_name, file_name)
    local out = {}
    -- package length
    out[#out+1] = int2buf(16 + string.len(file_name))
    -- cmd
    out[#out+1] = string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE)
    -- status
    out[#out+1] = "\00"
    -- group_name
    out[#out+1] = fix_string(group_name, 16)
    -- file name
    out[#out+1] = file_name
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(tracker_timeout)
    -- connect tracker
    local ok, err = sock:connect(tracker_host, tracker_port)
    if not ok then
        return nil, err
    end
    -- send request
    local bytes, err = sock:send(table.concat(out))
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read body
    local res = read_tracket_result(sock, hdr)
    -- keepalive
    sock:setkeepalive(3600*1000, tracker_keepalive)
    return res
end

function query_delete_storage(fileid)
    local pos = fileid:find('/')
    if not pos then
        return nil
    else
        local group_name = fileid:sub(1, pos-1)
        local file_name  = fileid:sub(pos + 1)
        local res = query_delete_storage_ex(group_name, file_name)
        res.file_name = file_name
        return res
    end
end

function do_delete_storage(storage)
    local out = {}
    out[#out+1] = int2buf(16 + string.len(storage.file_name))
    out[#out+1] = string.char(STORAGE_PROTO_CMD_DELETE_FILE)
    out[#out+1] = "\00"
    -- group name
    out[#out+1] = fix_string(storage.group_name, 16)
    -- file name
    out[#out+1] = storage.file_name
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(storage_timeout)
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    local bytes, err = sock:send(table.concat(out))
    if not bytes then
        ngx.log(ngx.ngx.ERR, "send body error")
        sock:close()
        ngx.exit(500)
    end
    -- read request header
    local hdr = read_fdfs_header(sock)
    sock:setkeepalive(storage_keepalive_timeout, storage_keepalive)
    return hdr
end
-- main
local method = ngx.var.arg_method
if method == 'delete' then
    local fileid = ngx.var.arg_fileid
    local storage = query_delete_storage(fileid)
    if storage then
        local res = do_delete_storage(storage)
        if res.status == 0 then
            ngx.say("OK")
        else
            ngx.say("ERR:(" .. res.status .. ")")
        end
    else
        ngx.say("not storage")
    end
elseif method == 'upload' then
    local ext_name = ngx.var.arg_ext_name
    local storage = query_upload_storage()
    local res = do_upload_storage(storage, ext_name)
    if res then
        ngx.say(string.format("%s/%s",res.group_name, res.file_name))
    else
        ngx.exit(406)
    end
else
    ngx.say('not input method')
end
