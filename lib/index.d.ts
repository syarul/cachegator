import type { Options } from "./types/index.js";
declare class CacheGator {
    private cacheType;
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
    private redisConnectPromise;
    private cacheExpiry;
    private forceCacheRegenerate;
    private tmpDir;
    constructor({ useRedis, redisOptions, tmpDir, batchReadSize, model, keyPrefix, debug, cacheExpiry, // default cache expiry in seconds
    forceCacheRegenerate, }: Options);
    private lazyLoadRedis;
    private closeRedisClient;
    setSplitter(splitter: any): void;
    split(config: any): void;
    private batchAggregator;
    literalQuery($literal: any[], query?: any[]): Promise<any>;
    generateCache(): Promise<string[]>;
    processChunk({ query, linesBuffer, chunkCount, combined, results, mergeFields, ignoreFields, }: {
        query: any[];
        linesBuffer: any[];
        chunkCount: number;
        combined: Map<string, any>;
        results: any[];
        mergeFields: string[];
        ignoreFields: string[];
    }): Promise<void>;
    aggregateCache({ keys, query, mergeFields, ignoreFields, }: {
        keys: string[];
        query: any[];
        mergeFields?: string[];
        ignoreFields?: string[];
    }): Promise<any[]>;
    private clearMemoryCache;
}
export default CacheGator;
//# sourceMappingURL=index.d.ts.map