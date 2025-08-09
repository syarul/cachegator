import mongoose from "mongoose";
const Schema = mongoose.Schema;

const ModelSchema = new Schema({
  i: {
    type: String,
    required: true,
  },
  e: {
    type: String,
    required: true,
  },
  applicationId: {
    type: String,
  },
  timestamp: {
    type: Date,
  },
  note: {
    type: String,
  },
  model: {
    type: String,
  },
  status: {
    type: String,
    enum: ["Success", "Failed"],
  },
});

mongoose.model("Model", ModelSchema);

const ModelSeriesSchema = new Schema({
  model: {
    type: String,
    required: true,
  },
  brand: {
    type: String,
    required: true,
  },
  series: {
    type: String,
    required: true,
  },
});

mongoose.model("ModelSeries", ModelSeriesSchema);
