import { createReadStream, createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, unlinkSync, writeFileSync, } from "fs";
import readline from "readline";
class CacheGator {
    cacheType;
    splitter;
    client;
    redisOptions;
    Model;
    batchReadSize;
    batchList = [];
    keyPrefix;
    debug;
    log = console.log.bind(console);
    error = console.error.bind(console);
    warn = console.warn.bind(console);
    redisConnectPromise = null;
    cacheExpiry; // ony use in redis, for in-memory cache, automatically flushed at the end
    forceCacheRegenerate = false;
    tmpDir;
    colors;
    constructor({ useRedis = false, redisOptions = {
        socket: {
            host: "127.0.0.1",
            port: 6379,
        },
    }, tmpDir = "./tmp", batchReadSize = 10000, model, keyPrefix = "CG", debug = false, cacheExpiry = 3600, // default cache expiry in seconds
    forceCacheRegenerate = false, // whether to force regenerate cache
     }) {
        this.cacheType = useRedis ? "redis" : "memory";
        this.redisOptions = redisOptions;
        this.Model = model; // placeholder for the model, to be set later
        this.batchReadSize = batchReadSize;
        this.keyPrefix = keyPrefix;
        this.debug = debug;
        this.cacheExpiry = cacheExpiry;
        this.forceCacheRegenerate = forceCacheRegenerate;
        this.tmpDir = tmpDir;
        if (this.cacheType === "memory") {
            if (!existsSync(tmpDir)) {
                try {
                    mkdirSync(tmpDir, { recursive: true });
                }
                catch (err) {
                    console.error("Failed to create tmp directory:", err);
                }
            }
        }
        else {
            this.lazyLoadRedis().catch((err) => {
                console.error("Failed to connect to Redis:", err);
            });
        }
        if (!this.debug) {
            this.log = () => { }; // disable logging if debug is false
        }
        this.colors = {
            GREEN: "\x1b[32m",
            RESET: "\x1b[0m",
            RED: "\x1b[31m",
            YELLOW: "\x1b[33m",
        };
    }
    async lazyLoadRedis() {
        // already connected → do nothing
        if (this.client?.isOpen) {
            return;
        }
        // connection already in progress → reuse the same promise
        if (this.redisConnectPromise) {
            return this.redisConnectPromise;
        }
        this.log("connecting to redis...");
        const { createClient } = await import("redis");
        // create only one client instance
        if (!this.client) {
            this.client = createClient(this.redisOptions);
        }
        // create the connection promise once
        this.redisConnectPromise = new Promise((resolve, reject) => {
            this.client.once("ready", () => {
                this.log("redis connected");
                resolve();
            });
            this.client.once("error", (err) => {
                this.error("redis connection failed:", err);
                this.redisConnectPromise = null; // allow retry
                reject(err);
            });
        });
        // kick off the connection
        this.client.connect().catch((err) => {
            this.redisConnectPromise = null; // allow retry if connect() fails
        });
        return this.redisConnectPromise;
    }
    async closeRedisClient() {
        this.log("closing redis client...");
        await this.lazyLoadRedis();
        if (this.client?.isOpen) {
            await this.client
                .close()
                .then(() => {
                this.log("redis client closed successfully.");
            })
                .catch(console.error);
        }
    }
    setSplitter(splitter) {
        this.splitter = splitter;
        this.log("splitter function set.");
    }
    split(config) {
        if (this.splitter) {
            this.batchList = this.splitter(config);
        }
        else {
            throw new Error("Splitter function is not set.");
        }
        if (this.batchList.length === 0) {
            throw new Error("No batches created from the splitter function.");
        }
        // this.log(
        //   "Splitter function is set and returned:",
        //   JSON.stringify(this.batchList.map((c) => c.payload)),
        // );
    }
    batchAggregator(query) {
        return this.Model.aggregate(query).cursor();
    }
    async literalQuery($literal, query = []) {
        const aggregator = [
            {
                $limit: 1,
            },
            {
                $facet: {
                    staticData: [
                        {
                            $project: {
                                data: {
                                    $literal,
                                },
                            },
                        },
                        { $unwind: "$data" },
                    ],
                },
            },
            { $unwind: "$staticData" },
            {
                $replaceRoot: {
                    newRoot: "$staticData.data",
                },
            },
            ...(query || []),
        ];
        return await this.Model.aggregate(aggregator).allowDiskUse(true);
    }
    async generateCache() {
        const batches = this.batchList;
        // wait for redis to connect
        if (this.cacheType === "redis") {
            await this.lazyLoadRedis();
        }
        let step = 0;
        const keys = [];
        let writeStreamResult = null;
        for (const { key, cache, type = "data", batchQuery } of batches) {
            const id = `${this.keyPrefix}:${key}`;
            const tmpFile = `${this.tmpDir}/${this.keyPrefix}_${key}.tmp`;
            keys.push(key);
            // force regenerate cache
            let cacheEntry;
            if (!this.forceCacheRegenerate) {
                if (this.cacheType === "memory") {
                    cacheEntry = existsSync(tmpFile) && readFileSync(tmpFile, "utf8");
                }
                else {
                    cacheEntry = await this.client.xRange(id, "0", "+", {
                        COUNT: 1,
                    });
                }
            }
            if (this.cacheType === "redis") {
                cacheEntry = await this.client.xRange(id, "0", "+", {
                    COUNT: 1,
                });
            }
            else {
                writeStreamResult = createWriteStream(tmpFile, {
                    flags: "a",
                });
            }
            // skip query if cache exist
            if (cacheEntry?.length) {
                this.log(`pre-loading ${this.colors.GREEN}${type}${this.colors.RESET} chunk ${this.colors.YELLOW}${step + 1}/${batches.length}${this.colors.RESET}...`);
                step++;
                continue;
            }
            this.log(`pre-processing ${type} chunk ${step + 1}/${batches.length}...`);
            const cursor = this.batchAggregator(batchQuery);
            let count = 0;
            let resolver;
            let rejecter;
            const promiseStream = new Promise((resolve, reject) => {
                resolver = resolve;
                rejecter = reject;
            });
            cursor.on("data", (doc) => {
                if (this.cacheType === "redis") {
                    this.client
                        .xAdd(id, "*", { json: JSON.stringify(doc) })
                        .catch((e) => {
                        this.error(e);
                    });
                }
                else {
                    writeStreamResult?.write(JSON.stringify(doc) + "\n");
                }
                count++;
            });
            cursor.on("end", () => {
                // this.log('aggregation streaming finished.');
                resolver();
            });
            cursor.on("error", (err) => {
                this.error("aggregation stream error:", err);
                rejecter(err);
            });
            await promiseStream;
            if (!cache) {
                if (this.cacheType === "redis") {
                    await this.client.expire(id, this.cacheExpiry);
                }
                else {
                    writeFileSync(`${this.tmpDir}/${this.keyPrefix}_${key}.remove.tmp`, "");
                }
            }
            step++;
        }
        if (this.cacheType === "memory") {
            writeStreamResult && writeStreamResult.end();
        }
        else {
            await this.closeRedisClient();
        }
        return keys;
    }
    async processChunk({ query, linesBuffer, batchCount, combined, results, mergeFields = [], ignoreFields = [], }) {
        this.log(`processing batch ${this.colors.YELLOW}%d${this.colors.RESET} :: ${this.colors.YELLOW}%d${this.colors.RESET} records processed...`, batchCount, batchCount * this.batchReadSize);
        const $literal = [];
        for (const line of linesBuffer) {
            const data = JSON.parse(line);
            if (Object.keys(data || {}).length) {
                if (data.timestamp) {
                    data.timestamp = new Date(data.timestamp);
                }
                $literal.push(data);
            }
        }
        let chunkResult = [];
        try {
            chunkResult = await this.literalQuery($literal, query);
        }
        catch (e) {
            console.error(e);
        }
        if (Array.isArray(chunkResult)) {
            if (mergeFields.length) {
                for (const chunk of chunkResult) {
                    const keys = Object.keys(chunk).filter((c) => ![...mergeFields, ...ignoreFields].includes(c));
                    const key = keys.reduce((acc, curr) => `${acc}${acc.length ? "-" : ""}${curr}-${chunk[curr]}`, "");
                    if (combined.has(key)) {
                        for (const field of mergeFields) {
                            const cf = chunk[field];
                            if (typeof combined.get(key)?.[field] === "number") {
                                combined.get(key)[field] = combined.get(key)[field] + cf;
                            }
                            else if (Array.isArray(combined.get(field)?.[field])) {
                                combined.get(key)[field].push(...cf);
                            }
                            else {
                                combined.get(key)[field] = cf;
                            }
                        }
                    }
                    else {
                        combined.set(key, chunk);
                    }
                }
            }
            else {
                results = results.concat(chunkResult);
            }
        }
        else {
            this.warn(`unexpected result processing batch ${this.colors.RED}%d${this.colors.RESET}`, batchCount);
        }
    }
    async aggregateCache({ keys, query, mergeFields = [], ignoreFields = [], }) {
        if (this.cacheType === "redis") {
            await this.lazyLoadRedis();
        }
        const combined = new Map();
        let results = [];
        let globalLineCount = 0;
        let linesBuffer = [];
        let batchCount = 0;
        let start = "0";
        for (const key of keys) {
            const id = `${this.keyPrefix}:${key}`;
            let entries = [];
            if (this.cacheType === "redis") {
                try {
                    entries = await this.client.xRange(id, start, "+", {
                        COUNT: this.batchReadSize,
                    });
                }
                catch (e) {
                    console.error(e);
                }
            }
            else {
                const rl = readline.createInterface({
                    input: createReadStream(`${this.tmpDir}/${this.keyPrefix}_${key}.tmp`),
                    crlfDelay: Infinity,
                });
                for await (const line of rl) {
                    entries.push({ message: { json: line } });
                }
                rl.close();
            }
            if (entries.length === 0)
                break;
            start = entries[entries.length - 1]?.id;
            for await (const { message } of entries) {
                linesBuffer.push(message.json);
                globalLineCount++;
                if (linesBuffer.length === this.batchReadSize) {
                    batchCount++;
                    await this.processChunk({
                        query,
                        linesBuffer,
                        batchCount,
                        combined,
                        results,
                        mergeFields,
                        ignoreFields,
                    });
                    linesBuffer = [];
                }
            }
            this.cacheType === "redis" && this.closeRedisClient();
        }
        if (linesBuffer.length > 0) {
            batchCount++;
            await this.processChunk({
                query,
                linesBuffer,
                batchCount,
                combined,
                results,
                mergeFields,
                ignoreFields,
            });
        }
        if (this.cacheType === "memory") {
            this.clearMemoryCache();
        }
        this.log(`${this.colors.GREEN}%d${this.colors.RESET} combined entries processed...`, combined.size);
        return mergeFields?.length ? Array.from(combined.values()) : results;
    }
    clearMemoryCache() {
        const files = readdirSync(this.tmpDir);
        const filters = files.filter((f) => /\.remove.tmp$/.test(f));
        for (const filter of filters) {
            let file = filter.slice(0, -11);
            const files = [
                `${this.tmpDir}/${file}.tmp`,
                `${this.tmpDir}/${file}.remove.tmp`,
            ];
            for (const file of files) {
                if (existsSync(file)) {
                    unlinkSync(file);
                }
            }
        }
    }
}
export default CacheGator;
//# sourceMappingURL=index.js.map