-- main
local fastdfs = require('resty.fastdfs')
local fdfs = fastdfs:new()
fdfs:set_tracker("192.168.85.249",22122)
fdfs:set_timeout(1000)
fdfs:set_tracker_keepalive(0, 100)
fdfs:set_storage_keepalive(0, 100)
local method = ngx.var.arg_method
if method == 'delete' then
    local fileid = ngx.var.arg_fileid
    local res = fdfs:do_delete(fileid)
    if not res then
        ngx.say("ERR")
    elseif res.status == 0 then
        ngx.say("OK")
    else
        ngx.say("ERR:(" .. res.status .. ")")
    end
elseif method == 'upload' then
    local ext_name = ngx.var.arg_ext_name
    local res = fdfs:do_upload(ext_name)
    if not res then
        ngx.say("ERR")
    elseif res then
        ngx.say(string.format("%s/%s",res.group_name, res.file_name))
    else
        ngx.exit(406)
    end
elseif method == 'upload_appender' then
    local ext_name = ngx.var.arg_ext_name
    local res = fdfs:do_upload_appender(ext_name)
    if not res then
        ngx.say("ERR")
    elseif res then
        ngx.say(string.format("%s/%s",res.group_name, res.file_name))
    else
        ngx.exit(406)
    end
elseif method == 'download' then
    local fileid = ngx.var.arg_fileid
    local data = fdfs:do_download(fileid)
    if not data then
        ngx.say("ERR")
    else
        ngx.print(data)
    end
elseif method == 'append' then
    local fileid = ngx.var.arg_fileid
    local res = fdfs:do_append(fileid)
    if not res then
        ngx.say("ERR")
    elseif res.status == 0 then
        ngx.say("OK")
    else
        ngx.say("ERR:(" .. res.status .. ")")
    end
else
    ngx.say('not input method')
end
