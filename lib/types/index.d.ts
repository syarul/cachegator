import type { RedisClientOptions } from "redis";
export type Options = {
    model: any;
    useRedis?: boolean;
    redisOptions?: RedisClientOptions;
    keyPrefix?: string;
    tmpDir?: string;
    batchReadSize?: number;
    debug?: boolean;
    cacheExpiry?: number;
    forceCacheRegenerate?: boolean;
    maxBytes?: number;
    persistentCaching?: boolean;
};
export type BatchObject = {
    key: string;
    type: string;
    cache: boolean;
    batchQuery: any[];
};
//# sourceMappingURL=index.d.ts.map