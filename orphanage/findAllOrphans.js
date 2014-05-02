printjson(Shard.active = Shard.allShards())
var results = Orphans.findAll()
for (ns in results) results[ns].listAll()
