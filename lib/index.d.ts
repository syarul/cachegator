import type { Options } from "./types/index.js";
declare class CacheGator {
    private cacheType;
    private persistentCaching;
    private splitter?;
    private client;
    private redisOptions;
    private Model;
    private batchReadSize;
    private batchList;
    private keyPrefix;
    private debug;
    private log;
    private error;
    private warn;
    private redisConnectPromise;
    private cacheExpiry;
    private forceCacheRegenerate;
    private tmpDir;
    private colors;
    private maxBytes;
    constructor({ useRedis, redisOptions, tmpDir, batchReadSize, model, keyPrefix, debug, cacheExpiry, // default cache expiry in seconds
    forceCacheRegenerate, // whether to force regenerate cache
    maxBytes, // actual max 17825792 but BSONObj require 16793600(16MB) limit size,
    persistentCaching, }?: Options);
    private lazyLoadRedis;
    private closeRedisClient;
    hashObject(obj: Record<string, any>): string;
    setSplitter(splitter: any): void;
    split(config: any): void;
    private batchAggregator;
    literalQuery($literal: any[], query?: any[]): Promise<any>;
    generateCache(): Promise<string[]>;
    private loadChunk;
    private processChunk;
    aggregateCache({ keys, query, mergeFields, ignoreFields, }: {
        keys: string[];
        query: any[];
        mergeFields?: string[];
        ignoreFields?: string[];
    }): Promise<any[]>;
    clearMemoryCache(): void;
    private clearLayerMemoryCache;
}
export default CacheGator;
export type { CacheGator };
//# sourceMappingURL=index.d.ts.map