import "./sampleModel";
import mongoose from "mongoose";
import "dotenv/config";
const Model = mongoose.model("Model");
const ModelSeries = mongoose.model("ModelSeries");

const seriesMap = new Map<string, any>();

export const generateMockData = async (count = 1000) => {
  const maxUnique = Math.floor(count / 5); // about 20% unique
  const start = new Date("2025-07-08T00:00:00.000Z").getTime();
  const end = new Date("2025-08-09T00:00:00.000Z").getTime();
  let c = 0;
  for (let i = 0; i < count; i++) {
    const randomI = Math.floor(Math.random() * maxUnique);
    const randomE = Math.floor(Math.random() * maxUnique);
    const randomTimestamp = new Date(start + Math.random() * (end - start));
    const modelName = `Model-${Math.floor(Math.random() * 9) + 1}`; // 9 possibilities
    let startTime = randomTimestamp;
    for (const ii of [1, 2, 3, 4, 5]) {
      const status = Math.random() < 0.9 ? "Success" : "Failed"; // 90% success
      const model = {
        i: `item-${randomI}`, // repeated possible
        e: `event-${randomE}`, // repeated possible
        applicationId: ii.toString(),
        timestamp: startTime,
        note: `Note for item ${i}`,
        model: modelName,
        status,
      };
      // models.push(model);
      const m = new Model(model);
      await m.save();
      c++;
      startTime = new Date(
        startTime.getTime() + Math.floor(Math.random() * 60000),
      ); // random time increment
    }
    console.log(`Saved ${c} models so far...`);

    const seriesItem = {
      model: modelName,
      brand: `Brand-${i % 3}`,
      series: `Series-${i % 4}`,
    };
    const key = `${seriesItem.model}-${seriesItem.brand}-${seriesItem.series}`;
    if (seriesMap.has(key)) {
      continue; // Skip if already exists
    }
    const s = new ModelSeries(seriesItem);
    await s.save();
    seriesMap.set(key, true);
  }
};

const client = await mongoose.connect(process.env.DB ?? "");

await generateMockData();

await client
  .disconnect()
  .then(() => {
    console.log("Mongoose connection closed.", client.connection.readyState);
  })
  .catch(console.error);
