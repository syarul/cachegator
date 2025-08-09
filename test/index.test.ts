import mongoose from "mongoose";
import "dotenv/config";
import "./sampleModel";
import CacheGator from "../src/index";
const Model = mongoose.model("Model");

const ctor = new CacheGator({
  useRedis: false,
  model: Model,
  debug: true,
});

function payload(startDate, endDate, opt) {
  return [
    {
      $match: {
        timestamp: {
          $gte: new Date(new Date(startDate).getTime() - 1000 * 60 * 60), // offset buffer to 1 hour
          $lte: new Date(new Date(endDate).getTime() + 1000 * 60 * 60), // offset buffer to 1 hour
        },
        applicationId: {
          $in: opt.applicationId ? opt.applicationId : ["3", "4"],
        },
      },
    },
    {
      $sort: {
        timestamp: -1,
      },
    },
    {
      $group: {
        _id: { i: "$i", e: "$e" },
        doc: { $push: "$$ROOT" },
      },
    },
    {
      $addFields: {
        app4: {
          $filter: {
            input: "$doc",
            as: "item",
            cond: {
              $and: [
                {
                  $eq: ["$$item.applicationId", "4"],
                },
              ],
            },
          },
        },
      },
    },
    {
      $addFields: {
        app3Filtered: {
          $filter: {
            input: "$doc",
            as: "item",
            cond: {
              $eq: ["$$item.applicationId", "3"],
            },
          },
        },
      },
    },
    {
      $addFields: {
        app3Sorted: {
          $sortArray: {
            input: "$app3Filtered",
            sortBy: { timestamp: -1 },
          },
        },
      },
    },
    {
      $addFields: {
        app34Filtered: {
          $concatArrays: ["$app3Filtered", "$app4"],
        },
      },
    },
    {
      $addFields: {
        app3: { $arrayElemAt: ["$app3Sorted", 0] },
      },
    },
    {
      $match: {
        "app3.status": {
          $ne: "Failed",
        },
      },
    },
    {
      $match: {
        doc: {
          $elemMatch: {
            applicationId: "3",
          },
        },
      },
    },
    {
      $match: {
        app34Filtered: {
          $elemMatch: {
            timestamp: {
              $gte: startDate,
              $lte: endDate,
            },
          },
        },
      },
    },
    {
      $addFields: {
        idxArr: {
          $range: [0, { $size: "$app3Sorted" }],
        },
      },
    },
    {
      $unwind: {
        path: "$idxArr",
        preserveNullAndEmptyArrays: false,
      },
    },
    {
      $addFields: {
        timestampApp3: {
          $arrayElemAt: ["$app3Sorted.timestamp", "$idxArr"],
        },
        app4Elem: { $arrayElemAt: ["$app4", "$idxArr"] },
        completedAfter: {
          $cond: [{ $gte: ["$idxArr", 1] }, true, false],
        },
      },
    },
    {
      $match: {
        app4Elem: { $exists: true },
      },
    },
    {
      $group: {
        _id: "$_id.e",
        uniqueI: {
          $addToSet: "$_id.i",
        },
        allI: {
          $push: "$_id.i",
        },
        allE: {
          $push: "$_id.e",
        },
        uniqueModel: {
          $addToSet: {
            $ifNull: ["$app1.model", "$app3.model", "$app4Elem.model"],
          },
        },
        timestamp: {
          $addToSet: "$timestampApp3",
        },
        count: {
          $sum: 1,
        },
      },
    },
    {
      $addFields: {
        iCount: {
          $sum: {
            $map: {
              input: {
                $setUnion: ["$allI", []],
              }, // unique values
              as: "val",
              in: {
                $let: {
                  vars: {
                    count: {
                      $size: {
                        $filter: {
                          input: "$allI",
                          cond: {
                            $eq: ["$$this", "$$val"],
                          },
                        },
                      },
                    },
                  },
                  in: {
                    $cond: [
                      { $gt: ["$$count", 1] },
                      { $subtract: ["$$count", 1] },
                      0,
                    ],
                  },
                },
              },
            },
          },
        },
        eCount: {
          $sum: {
            $map: {
              input: {
                $setUnion: ["$allE", []],
              }, // unique values
              as: "val",
              in: {
                $let: {
                  vars: {
                    count: {
                      $size: {
                        $filter: {
                          input: "$allE",
                          cond: {
                            $eq: ["$$this", "$$val"],
                          },
                        },
                      },
                    },
                  },
                  in: {
                    $cond: [
                      { $gt: ["$$count", 1] },
                      { $subtract: ["$$count", 1] },
                      0,
                    ],
                  },
                },
              },
            },
          },
        },
      },
    },
    {
      $lookup: {
        from: "modelseries",
        localField: "uniqueModel.0",
        foreignField: "model",
        as: "model",
      },
    },
  ];
}

function splitDateRange({
  startDate,
  endDate,
  offset = 8,
  type = "data",
  payload = () => [],
  opt = {},
}: any) {
  let start = new Date(startDate);
  let end = new Date(endDate);

  const offsetHour = 24 - offset;
  const intervals: {
    startDate: string;
    endDate: string;
    key: string;
    cache: boolean;
    batchQuery: Record<string, any>[];
  }[] = [];

  let intervalStart = new Date(start);

  const current = new Date();
  const cYear = current.getUTCFullYear();
  const cMonth = current.getUTCMonth();
  const cDate = current.getUTCDate();

  const intervalCurrent = new Date(
    Date.UTC(cYear, cMonth, cDate, offsetHour, 0, 0, 0),
  );

  while (intervalStart < end) {
    const year = intervalStart.getUTCFullYear();
    const month = intervalStart.getUTCMonth();
    const date = intervalStart.getUTCDate();

    // interval end at offsetHour on the last day of current month
    let intervalEnd = new Date(
      Date.UTC(year, month, date, offsetHour, 0, 0, 0),
    );

    // move intervalEnd to offsetHour on first day of next month
    if (intervalEnd <= intervalStart) {
      intervalEnd = new Date(
        Date.UTC(year, month, date + 1, offsetHour, 0, 0, 0),
      );
    }

    // when endDate exceed the end range
    let intervalEndCache;
    if (intervalEnd > end) {
      intervalEndCache = intervalEnd;
      intervalEnd = new Date(end);
    }

    const key = ctor.hashObject({
      startDate: intervalStart.toISOString(),
      endDate: intervalEnd.toISOString(),
      type,
    });

    const config = {
      startDate: intervalStart.toISOString(),
      endDate: intervalEnd.toISOString(),
      key,
      cache: intervalCurrent.toISOString() !== intervalEnd.toISOString(),
      batchQuery: payload(startDate, endDate, opt),
      type,
    };

    intervals.push(config);

    intervalStart = new Date(intervalEnd);
  }

  return intervals;
}

const client = await mongoose.connect(process.env.DB ?? "");

async function getData(params: any) {
  try {
    const { from, to, limit } = params;
    console.log(params);
    const fromDate = new Date(new Date(from).getTime() - 1000 * 60 * 60 * 8); // convert to UTC
    const toDate = new Date(new Date(to).getTime() - 1000 * 60 * 60 * 8); // convert to UTC

    const opt: any = {
      startDate: fromDate,
      endDate: toDate,
      applicationId: ["1", "3", "4"],
      type: "data_sample",
    };

    ctor.setSplitter(splitDateRange);

    opt.payload = payload;

    ctor.split(opt);

    const cacheKeys = await ctor.generateCache();
    // console.log(cacheKeys);
    const postProcessingQuery = [
      {
        $addFields: {
          brand: {
            $replaceAll: {
              input: {
                $toLower: {
                  $ifNull: [
                    {
                      $arrayElemAt: ["$model.brand", 0],
                    },
                    "NA",
                  ],
                },
              },
              find: " ",
              replacement: "",
            },
          },
        },
      },
      {
        $group: {
          _id: `$brand`,
          iCount: {
            $sum: "$iCount",
          },
          eCount: {
            $sum: "$eCount",
          },
          total: {
            $sum: "$count",
          },
        },
      },
      {
        $facet: {
          groupData: [{ $match: {} }], // all groups (pass-through)
          totalCount: [
            {
              $group: {
                _id: null,
                total: { $sum: "$total" },
              },
            },
          ],
        },
      },
      {
        $unwind: {
          path: "$totalCount",
          preserveNullAndEmptyArrays: true,
        },
      },
      {
        $unwind: {
          path: "$groupData",
          preserveNullAndEmptyArrays: true,
        },
      },
      {
        $project: {
          _id: "$groupData._id",
          brand: {
            $cond: [
              { $eq: [{ $toLower: "$groupData._id" }, "na"] },
              "NA",
              {
                $concat: [
                  {
                    $substrCP: [
                      { $toUpper: { $ifNull: ["$groupData._id", ""] } },
                      0,
                      1,
                    ],
                  },
                  {
                    $substrCP: [
                      { $ifNull: ["$groupData._id", ""] },
                      1,
                      {
                        $max: [
                          {
                            $subtract: [
                              {
                                $strLenCP: { $ifNull: ["$groupData._id", ""] },
                              },
                              1,
                            ],
                          },
                          1,
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          total: "$totalCount.total",
          totalCount: "$groupData.total",
          iCount: "$groupData.iCount",
          eCount: "$groupData.eCount",
        },
      },
    ];
    const handleData = await ctor.aggregateCache({
      keys: cacheKeys,
      query: postProcessingQuery,
      mergeFields: ["total", "totalCount", "iCount", "eCount"],
    });
    console.log(JSON.stringify(handleData));
    const result = await ctor.literalQuery(handleData, [
      {
        $group: {
          _id: "$brand",
          total: {
            $first: "$total",
          },
          totalCount: {
            $sum: "$totalCount",
          },
          iCount: {
            $sum: "$iCount",
          },
          eCount: {
            $sum: "$eCount",
          },
        },
      },
      {
        $addFields: {
          percentCount: {
            $multiply: [
              {
                $divide: ["$totalCount", "$total"],
              },
              100,
            ],
          },
        },
      },
      {
        $project: {
          _id: 0,
          brand: "$_id",
          totalCount: 1,
          iCount: 1,
          eCount: 1,
          percentCount: 1,
        },
      },
      {
        $sort: {
          totalCount: -1,
        },
      },
      {
        $limit: limit,
      },
    ]);
    console.log(result);
  } catch (e) {
    console.error(e);
  }
}

const options = {
  groupBy: "brand",
  // from: "2025-08-08T00:00:00.000Z",
  // to: "2025-08-08T23:59:59.999Z",
  from: "2025-07-08T00:00:00.000Z",
  to: "2025-08-09T00:00:00.000Z",
  limit: 9,
};

console.log("starting query...");

try {
  const q = await getData(options);
  console.log(q);
} catch (error) {
  console.error("error during query:", error);
}

await client
  .disconnect()
  .then(() => {
    console.log("Mongoose connection closed.", client.connection.readyState);
  })
  .catch(console.error);
