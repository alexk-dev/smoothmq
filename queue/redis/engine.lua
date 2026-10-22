-- All transitions use Redis time and execute atomically on one Redis primary.
-- Redis Cluster is intentionally unsupported. Every loop has a fixed bound.
local root, op, name = KEYS[1], ARGV[1], ARGV[2]
local a = cjson.decode(ARGV[3])
local clock = redis.call('TIME')
local now = tonumber(clock[1])*1000 + math.floor(tonumber(clock[2])/1000)
local function key(q,s) return root..':'..q..':'..s end
local function fail(code,msg) return redis.error_reply('SMQ|'..code..'|'..msg) end
local function meta(q)
 local raw=redis.call('GET',key(q,'meta')); if raw then return cjson.decode(raw) end
end
local function message(q,id)
 local raw=redis.call('HGET',key(q,'messages'),id); if raw then return cjson.decode(raw) end
end
local function remove(q,id)
 redis.call('HDEL',key(q,'messages'),id)
 redis.call('ZREM',key(q,'ready'),id); redis.call('ZREM',key(q,'inflight'),id); redis.call('ZREM',key(q,'sent'),id)
end
local function save(q,m)
 redis.call('HSET',key(q,'messages'),m.id,cjson.encode(m))
end
local function array(items)
 local encoded={}; for _,v in ipairs(items) do encoded[#encoded+1]=cjson.encode(v) end
 return '['..table.concat(encoded,',')..']'
end
local function cleanup(q,props)
 local expired=redis.call('ZRANGEBYSCORE',key(q,'sent'),'-inf',now-tonumber(props.attrs.MessageRetentionPeriod)*1000,'LIMIT',0,256)
 for _,id in ipairs(expired) do remove(q,id) end
 local released=redis.call('ZRANGEBYSCORE',key(q,'inflight'),'-inf',now,'LIMIT',0,256)
 for _,id in ipairs(released) do
  local m=message(q,id)
  redis.call('ZREM',key(q,'inflight'),id)
  if m then m.receipt=''; save(q,m); redis.call('ZADD',key(q,'ready'),m.available,id) end
 end
end
local function arn(q) return a.arnPrefix..q end
local function permitted(target,sourceArn)
 if not target.attrs.RedriveAllowPolicy then return true end
 local policy=cjson.decode(target.attrs.RedriveAllowPolicy)
 if policy.redrivePermission=='allowAll' then return true end
 if policy.redrivePermission=='denyAll' then return false end
 for _,allowed in ipairs(policy.sourceQueueArns or {}) do if allowed==sourceArn then return true end end
 return false
end
local function validateTarget(props)
 if not props.attrs.RedrivePolicy then return nil end
 local policy=cjson.decode(props.attrs.RedrivePolicy)
 local targetName=string.match(policy.deadLetterTargetArn,':([^:]+)$')
 if targetName==name then return fail('InvalidParameterValue','A queue cannot be its own dead-letter queue') end
 local target=meta(targetName)
 if not target then return fail('InvalidParameterValue','Dead-letter target does not exist') end
 if not permitted(target,arn(name)) then return fail('InvalidParameterValue','Dead-letter target denies this source') end
 -- Reject cycles before mutating configuration; bound traversal too.
 local seen={[name]=true}; local nextName=targetName
 for i=1,100 do
  if seen[nextName] then return fail('InvalidParameterValue','Dead-letter queue cycle') end
  seen[nextName]=true; local p=meta(nextName)
  if not p or not p.attrs.RedrivePolicy then return nil end
  nextName=string.match(cjson.decode(p.attrs.RedrivePolicy).deadLetterTargetArn,':([^:]+)$')
 end
 return fail('InvalidParameterValue','Dead-letter chain is too long')
end
if op=='create' then
 local existing=meta(name)
 if existing then
  for k,v in pairs(a.requested) do if existing.attrs[k]~=v then return fail('QueueNameExists','Queue exists with different attributes') end end
  return '{}'
 end
 if redis.call('SCARD',root..':queues')>=1000 then return fail('OverLimit','At most 1000 queues per account') end
 local props={attrs=a.attrs,tags=a.tags,generation=a.generation,created=math.floor(now/1000),modified=math.floor(now/1000)}
 local err=validateTarget(props); if err then return err end
 redis.call('SET',key(name,'meta'),cjson.encode(props)); redis.call('SADD',root..':queues',name)
 return '{}'
end
if op=='list' then return array(redis.call('SMEMBERS',root..':queues')) end
local props=meta(name)
if not props and op~='task-list' and op~='task-cancel' and op~='task-step' then return fail('QueueDoesNotExist','Queue does not exist') end
if op=='attrs' then
 cleanup(name,props)
 local attrs=props.attrs
 attrs.QueueArn=arn(name); attrs.CreatedTimestamp=tostring(props.created); attrs.LastModifiedTimestamp=tostring(props.modified)
 attrs.ApproximateNumberOfMessages=tostring(redis.call('ZCOUNT',key(name,'ready'),'-inf',now))
 attrs.ApproximateNumberOfMessagesDelayed=tostring(redis.call('ZCOUNT',key(name,'ready'),'('..now,'+inf'))
 attrs.ApproximateNumberOfMessagesNotVisible=tostring(redis.call('ZCARD',key(name,'inflight')))
 return cjson.encode(attrs)
end
if op=='set' then
 for k,v in pairs(a.attrs) do if v=='' and (k=='RedrivePolicy' or k=='RedriveAllowPolicy') then props.attrs[k]=nil else props.attrs[k]=v end end
 local err=validateTarget(props); if err then return err end
 props.modified=math.floor(now/1000); redis.call('SET',key(name,'meta'),cjson.encode(props)); return '{}'
end
if op=='tags' then
 for k,v in pairs(a.add) do props.tags[k]=v end
 for _,k in ipairs(a.remove) do props.tags[k]=nil end
 local n=0; for _ in pairs(props.tags) do n=n+1 end
 if n>50 then return fail('InvalidParameterValue','At most 50 tags are allowed') end
 redis.call('SET',key(name,'meta'),cjson.encode(props)); return cjson.encode(props.tags)
end
if op=='purge' or op=='delete' then
 -- UNLINK detaches whole structures atomically and reclaims large queues off-thread.
 redis.call('UNLINK',key(name,'messages'),key(name,'ready'),key(name,'inflight'),key(name,'sent'))
 if op=='delete' then redis.call('DEL',key(name,'meta')); redis.call('SREM',root..':queues',name)
 else props.generation=a.generation; redis.call('SET',key(name,'meta'),cjson.encode(props)) end
 return '{}'
end
if op=='send' then
 if (tonumber(a.kv.__sqs_size) or #a.body)>tonumber(props.attrs.MaximumMessageSize) then return fail('InvalidParameterValue','Message exceeds queue maximum size') end
 local id=tostring(redis.call('INCR',root..':seq'))
 local delay=a.delay; if delay==nil then delay=tonumber(props.attrs.DelaySeconds) end
 local m={id=id,body=a.body,kv=a.kv,sent=now,first=0,count=0,available=now+delay*1000,receipt='',generation=props.generation,source=''}
 save(name,m); redis.call('ZADD',key(name,'sent'),now,id); redis.call('ZADD',key(name,'ready'),m.available,id)
 return id
end
if op=='maintain' then cleanup(name,props); return '{}' end
if op=='receive' then
 cleanup(name,props)
 if a.generation and a.generation~=props.generation then return fail('QueueDoesNotExist','Queue changed while polling') end
 local result={}; local candidates=redis.call('ZRANGEBYSCORE',key(name,'ready'),'-inf',now,'LIMIT',0,256)
 local policy, targetName, target
 if props.attrs.RedrivePolicy then
  policy=cjson.decode(props.attrs.RedrivePolicy); targetName=string.match(policy.deadLetterTargetArn,':([^:]+)$'); target=meta(targetName)
  if not target or not permitted(target,arn(name)) then return fail('InvalidParameterValue','Dead-letter target is missing or denies source') end
 end
 for _,id in ipairs(candidates) do
  if #result>=a.count then break end
  local m=message(name,id)
  if not m or m.sent+tonumber(props.attrs.MessageRetentionPeriod)*1000<=now then remove(name,id)
  elseif policy and m.count>=tonumber(policy.maxReceiveCount) then
   -- Destination is fully validated before the first source mutation.
   m.receipt=''; m.count=0; m.first=0; m.available=now; m.source=arn(name); m.generation=target.generation
   save(targetName,m); redis.call('ZADD',key(targetName,'sent'),m.sent,id); redis.call('ZADD',key(targetName,'ready'),now,id); remove(name,id)
  else
   local visibility=a.visibility; if visibility==nil then visibility=tonumber(props.attrs.VisibilityTimeout) end
   m.count=m.count+1; if m.first==0 then m.first=now end
   m.leaseStart=now; m.available=now+visibility*1000; m.receipt=id..':'..a.tokens[#result+1]; m.generation=props.generation
   save(name,m); redis.call('ZREM',key(name,'ready'),id); redis.call('ZADD',key(name,'inflight'),m.available,id); result[#result+1]=m
  end
 end
 return array(result)
end
if op=='ack' or op=='visibility' then
 local id=string.match(a.receipt,'^(%d+):')
 local m=id and message(name,id)
 if not m or m.receipt~=a.receipt or m.generation~=props.generation or m.available<=now or m.sent+tonumber(props.attrs.MessageRetentionPeriod)*1000<=now then
  return fail('ReceiptHandleIsInvalid','Receipt is invalid, expired, or belongs to an older delivery')
 end
 if op=='ack' then remove(name,id)
 else
  if now+a.visibility*1000>m.leaseStart+43200000 then return fail('InvalidParameterValue','Visibility exceeds the twelve-hour delivery limit') end
  m.available=now+a.visibility*1000
  if a.visibility==0 then m.receipt=''; redis.call('ZREM',key(name,'inflight'),id); redis.call('ZADD',key(name,'ready'),now,id)
  else redis.call('ZADD',key(name,'inflight'),m.available,id) end
  save(name,m)
 end
 return '{}'
end
if op=='inspect' then return redis.call('HGET',key(name,'messages'),a.id) or '{}' end
if op=='browse' then
 cleanup(name,props)
 local ids=redis.call('ZRANGE',key(name,'sent'),0,99); local result={}
 for _,id in ipairs(ids) do local m=message(name,id); if m then result[#result+1]=m end end
 return array(result)
end
if op=='generation' then return props.generation end
-- Message move tasks are durable, bounded, and cooperatively executed by all facades.
if op=='task-start' then
 local referenced=false
 for _,other in ipairs(redis.call('SMEMBERS',root..':queues')) do
  local p=meta(other); if p and p.attrs.RedrivePolicy and cjson.decode(p.attrs.RedrivePolicy).deadLetterTargetArn==arn(name) then referenced=true;break end
 end
 if not referenced then return fail('InvalidParameterValue','Move source must be a configured dead-letter queue') end
 -- Cap total retained tasks, pruning terminal history before accepting a new task.
 local history=redis.call('ZRANGE',root..':task-index',0,99)
 for _,id in ipairs(history) do
  if redis.call('HLEN',root..':tasks')<100 then break end
  local raw=redis.call('HGET',root..':tasks',id)
  if not raw or cjson.decode(raw).Status~='RUNNING' then redis.call('HDEL',root..':tasks',id);redis.call('ZREM',root..':task-index',id) end
 end
 if redis.call('HLEN',root..':tasks')>=100 then return fail('OverLimit','At most 100 active move tasks per account') end
 local active=redis.call('HGETALL',root..':tasks')
 for i=2,#active,2 do local t=cjson.decode(active[i]); if t.SourceArn==arn(name) and t.Status=='RUNNING' then return fail('UnsupportedOperation','A move task is already running for this source') end end
 local dest=a.destination
 if dest~='' and not meta(dest) then return fail('QueueDoesNotExist','Move destination does not exist') end
 if dest==name then return fail('InvalidParameterValue','Source and destination must differ') end
 local t={TaskHandle=a.handle,SourceArn=arn(name),DestinationArn=dest~='' and arn(dest) or '',Status='RUNNING',StartedTimestamp=now,ApproximateNumberOfMessagesMoved=0,ApproximateNumberOfMessagesToMove=redis.call('HLEN',key(name,'messages')),MaxNumberOfMessagesPerSecond=a.rate,source=name,destination=dest,generation=props.generation,lastStep=0}
 redis.call('HSET',root..':tasks',a.handle,cjson.encode(t)); redis.call('ZADD',root..':task-index',now,a.handle)
 -- Keep bounded history (at most 100 tasks per account); active task count is capped too.
 return cjson.encode({TaskHandle=a.handle})
end
if op=='task-list' then
 local result={}; local ids=redis.call('ZREVRANGE',root..':task-index',0,99)
 for _,id in ipairs(ids) do local raw=redis.call('HGET',root..':tasks',id); if raw then local t=cjson.decode(raw); if t.SourceArn==a.sourceArn then result[#result+1]=t end end end
 return '{"Results":'..array(result)..'}'
end
if op=='task-cancel' then
 local raw=redis.call('HGET',root..':tasks',a.handle); if not raw then return fail('ResourceNotFoundException','Move task does not exist') end
 local t=cjson.decode(raw)
 if t.SourceArn~=a.sourceArn then return fail('AccessDenied','Task source does not match') end
 if t.Status~='RUNNING' then return fail('UnsupportedOperation','Move task is not running') end
 t.Status='CANCELLED'; redis.call('HSET',root..':tasks',a.handle,cjson.encode(t)); return cjson.encode({ApproximateNumberOfMessagesMoved=t.ApproximateNumberOfMessagesMoved})
end
if op=='task-step' then
 local raw=redis.call('HGET',root..':tasks',a.handle); if not raw then return '{}' end
 local t=cjson.decode(raw)
 if t.Status~='RUNNING' or now-t.lastStep<1000 then return '{}' end
 local function storeTask() redis.call('HSET',root..':tasks',a.handle,cjson.encode(t)) end
 if not props or props.generation~=t.generation then t.Status='FAILED'; t.FailureReason='Source queue changed'; storeTask(); return '{}' end
 cleanup(name,props); t.lastStep=now
 local ids=redis.call('ZRANGEBYSCORE',key(name,'ready'),'-inf',now,'LIMIT',0,math.min(t.MaxNumberOfMessagesPerSecond,256))
 local moves={}
 for _,id in ipairs(ids) do
  local m=message(name,id)
  if m and m.sent+tonumber(props.attrs.MessageRetentionPeriod)*1000<=now then remove(name,id)
  elseif m then
   local dest=t.destination; if dest=='' then dest=string.match(m.source,':([^:]+)$') end
   local p=dest and meta(dest)
   if not p or dest==name then t.Status='FAILED'; t.FailureReason='Original destination is unavailable'; storeTask(); return '{}' end
   moves[#moves+1]={id=id,message=m,destination=dest,props=p}
  end
 end
 for _,move in ipairs(moves) do
  local m=move.message; m.id=tostring(redis.call('INCR',root..':seq')); m.sent=now; m.first=0; m.count=0; m.receipt=''; m.source=''; m.available=now; m.generation=move.props.generation
  save(move.destination,m); redis.call('ZADD',key(move.destination,'sent'),now,m.id); redis.call('ZADD',key(move.destination,'ready'),now,m.id); remove(name,move.id); t.ApproximateNumberOfMessagesMoved=t.ApproximateNumberOfMessagesMoved+1
 end
 if redis.call('HLEN',key(name,'messages'))==0 then t.Status='COMPLETED' end
 storeTask(); return '{}'
end
return fail('UnsupportedOperation','Backend operation is unsupported')
