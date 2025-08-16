"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = require("fs");
const readline = require("readline");
const crypto = require("crypto");
class CacheGator {
    constructor({ useRedis = false, redisOptions = {
        socket: {
            host: "127.0.0.1",
            port: 6379,
        },
    }, tmpDir = "./tmp", batchReadSize = 10000, model, keyPrefix = "CG", debug = false, cacheExpiry = 3600, // default cache expiry in seconds
    forceCacheRegenerate = false, // whether to force regenerate cache
    maxBytes = 16792600, // actual max 17825792 but BSONObj require 16793600(16MB) limit size,
    persistentCaching = false, } = {}) {
        this.batchList = [];
        this.log = console.log.bind(console);
        this.error = console.error.bind(console);
        this.warn = console.warn.bind(console);
        this.redisConnectPromise = null;
        this.forceCacheRegenerate = false;
        this.cacheType = useRedis ? "redis" : "memory";
        this.persistentCaching = persistentCaching;
        this.redisOptions = redisOptions;
        this.Model = model; // placeholder for the model, to be set later
        this.batchReadSize = batchReadSize;
        this.keyPrefix = keyPrefix;
        this.debug = debug;
        this.cacheExpiry = cacheExpiry;
        this.forceCacheRegenerate = forceCacheRegenerate;
        this.tmpDir = tmpDir;
        this.maxBytes = maxBytes;
        if (this.cacheType === "memory") {
            if (!(0, fs_1.existsSync)(tmpDir)) {
                try {
                    (0, fs_1.mkdirSync)(tmpDir, { recursive: true });
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
        const { createClient } = await Promise.resolve().then(() => require("redis"));
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
    hashObject(obj) {
        return crypto
            .createHash("sha256")
            .update(JSON.stringify(obj))
            .digest("hex");
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
                    cacheEntry = (0, fs_1.existsSync)(tmpFile) && (0, fs_1.readFileSync)(tmpFile, "utf8");
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
                writeStreamResult = (0, fs_1.createWriteStream)(tmpFile, {
                    flags: "a",
                });
            }
            // skip query if cache exist
            if (cacheEntry?.length) {
                this.log(`pre-loading ${this.colors.GREEN}${type}${this.colors.RESET} chunk ${this.colors.YELLOW}${step + 1}/${batches.length}${this.colors.RESET}...`);
                step++;
                continue;
            }
            this.log(`pre-processing ${this.colors.GREEN}${type}${this.colors.RESET} chunk ${this.colors.YELLOW}${step + 1}/${batches.length}${this.colors.RESET}...`);
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
                    (0, fs_1.writeFileSync)(`${this.tmpDir}/${this.keyPrefix}_${key}.remove.tmp`, "");
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
    async loadChunk({ linesBuffer, query, batchCount, bytesCount }) {
        const cacheKey = this.hashObject({
            query,
            batchCount,
            bytesCount,
        });
        let chunkCache;
        if (this.cacheType === "memory") {
            const memCache = `${this.tmpDir}/${this.keyPrefix}_${cacheKey}.layer.tmp`;
            if ((0, fs_1.existsSync)(memCache)) {
                chunkCache = (0, fs_1.readFileSync)(memCache, "utf8");
            }
        }
        else {
            chunkCache = await this.client.hGet(`${this.keyPrefix}_${cacheKey}`, "LAYER");
        }
        if (chunkCache?.length) {
            this.log(`loading batch ${this.colors.YELLOW}%d${this.colors.RESET} :: ${this.colors.YELLOW}%d${this.colors.RESET} bytes processed (${this.colors.YELLOW}%d${this.colors.RESET} records)...`, batchCount, bytesCount, linesBuffer.length);
            return JSON.parse(chunkCache);
        }
        this.log(`processing batch ${this.colors.YELLOW}%d${this.colors.RESET} :: ${this.colors.YELLOW}%d${this.colors.RESET} bytes processed (${this.colors.YELLOW}%d${this.colors.RESET} records)...`, batchCount, bytesCount, linesBuffer.length);
        let chunkResult = [];
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
        try {
            chunkResult = await this.literalQuery($literal, query);
        }
        catch (e) {
            console.error(e);
        }
        if (this.cacheType === "memory") {
            (0, fs_1.writeFileSync)(`${this.tmpDir}/${this.keyPrefix}_${cacheKey}.layer.tmp`, JSON.stringify(chunkResult)); // persistent, expired is trigger on next trigger
        }
        else {
            await this.client.hSet(`${this.keyPrefix}_${cacheKey}`, "LAYER", JSON.stringify(chunkResult));
            await this.client.expire(`${this.keyPrefix}_${cacheKey}`, this.cacheExpiry);
        }
        return chunkResult;
    }
    async processChunk({ query, linesBuffer, batchCount, bytesCount, combined, results, mergeFields = [], ignoreFields = [], }) {
        const chunkResult = await this.loadChunk({
            linesBuffer,
            query,
            batchCount,
            bytesCount,
        });
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
        else {
            // clear layer cache first
            this.clearLayerMemoryCache();
        }
        const combined = new Map();
        let results = [];
        let globalLineCount = 0;
        let linesBuffer = [];
        let batchCount = 0;
        let start = "0";
        let bytesCount = 0;
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
                    input: (0, fs_1.createReadStream)(`${this.tmpDir}/${this.keyPrefix}_${key}.tmp`),
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
                bytesCount += Buffer.byteLength(JSON.stringify(message.json), "utf8");
                globalLineCount++;
                if (linesBuffer.length === this.batchReadSize ||
                    bytesCount > this.maxBytes) {
                    batchCount++;
                    await this.processChunk({
                        query,
                        linesBuffer,
                        batchCount,
                        bytesCount,
                        combined,
                        results,
                        mergeFields,
                        ignoreFields,
                    });
                    linesBuffer = [];
                    bytesCount = 0; // reset buffer count
                }
            }
            // this.cacheType === "redis" && this.closeRedisClient();
        }
        if (linesBuffer.length > 0) {
            batchCount++;
            await this.processChunk({
                query,
                linesBuffer,
                batchCount,
                bytesCount,
                combined,
                results,
                mergeFields,
                ignoreFields,
            });
        }
        if (this.cacheType === "memory" && !this.persistentCaching) {
            this.clearMemoryCache();
        }
        else {
            this.cacheType === "redis" && this.closeRedisClient();
        }
        this.log(`${this.colors.GREEN}%d${this.colors.RESET} combined entries processed...`, combined.size);
        return mergeFields?.length ? Array.from(combined.values()) : results;
    }
    clearMemoryCache() {
        const files = (0, fs_1.readdirSync)(this.tmpDir);
        const filters = files.filter((f) => /\.remove.tmp$/.test(f));
        for (const filter of filters) {
            try {
                let file = filter.slice(0, -11);
                const files = [
                    `${this.tmpDir}/${file}.tmp`,
                    `${this.tmpDir}/${file}.remove.tmp`,
                ];
                for (const file of files) {
                    if ((0, fs_1.existsSync)(file)) {
                        (0, fs_1.unlinkSync)(file);
                    }
                }
            }
            catch (err) {
                console.error(err.message);
            }
        }
    }
    clearLayerMemoryCache() {
        const files = (0, fs_1.readdirSync)(this.tmpDir);
        // LAYER clear cache base on fs.stat
        const layerCaches = files.filter((f) => /\.layer.tmp$/.test(f));
        const now = Date.now();
        let count = 0;
        for (const layerCache of layerCaches) {
            let file = `${this.tmpDir}/${layerCache}`;
            try {
                const stats = (0, fs_1.statSync)(file);
                const mtime = stats.mtime.getTime(); // last modified time in ms
                const age = (now - mtime) / (this.cacheExpiry * 1000) - (now - mtime);
                if (age > 1) {
                    (0, fs_1.unlinkSync)(file);
                    count++;
                }
            }
            catch (err) {
                console.error(err.message);
            }
        }
        this.log(`${this.colors.YELLOW}%d${this.colors.RESET} total cache layer(s) cleared...`, count);
    }
}
exports.default = CacheGator;
//# sourceMappingURL=index.js.map