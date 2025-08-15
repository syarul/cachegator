import type { RedisClientOptions } from "redis";

export type Options = {
  model: any; // target mongoose model or the collection name
  useRedis?: boolean; // whether to use Redis as cache or default memory cache
  redisOptions?: RedisClientOptions; // options for redis client
  keyPrefix?: string; // prefix for cache keys
  tmpDir?: string; // memory cache, the directory to store temporary files
  batchReadSize?: number; // size of each batch read from the caches
  debug?: boolean; // debugging mode
  cacheExpiry?: number; // cache expiry in seconds, default is 3600 seconds (1 hour)
  forceCacheRegenerate?: boolean; // whether to force regenerate cache
  maxBytes?: number;
  // persistentCaching default false, if set to true you need to call ctor.clearMemoryCache()
  // manually, if you want to do extra custom processing before clearing the cache
  // or if you wish to keep it indefinitely
  persistentCaching?: boolean;
};

// BatchObject required parameters from calling the splitter function
export type BatchObject = {
  key: string;
  type: string;
  cache: boolean;
  batchQuery: any[];
};
