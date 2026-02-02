# DistCache
A distributed in-memory cache service

## About
This go service is a self-distributing cache service, that uses in memory storage it looks for anything in the .internal domain for port 42069 and asks if it is a cache service

Then adds that to its internal register and grabs a replica of the cache

### LLM Usage
This was written 95% with claude

