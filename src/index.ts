import { MongoClient } from "mongodb";

(async () => {
  const first = require("../json/first.json");
  const second = require("../json/second.json");

  const url = "mongodb://localhost:27017";
  const dbName = "test";

  const mongoClient = new MongoClient(url, { useUnifiedTopology: true });

  try {
    const client = await mongoClient.connect();
    const db = client.db(dbName);

    const firstCollection = db.collection("first");
    const secondCollection = db.collection("second");
    const thirdCollection = db.collection("third");

    await firstCollection.deleteMany({});
    await secondCollection.deleteMany({});
    await thirdCollection.deleteMany({});

    await firstCollection.insertMany(first);
    await secondCollection.insertMany(second);

    await firstCollection.aggregate([
      {
         $set: { 
           longitude: { $first: "$location.ll" }, 
           latitude: { $arrayElemAt: ["$location.ll", 1] },
           amount_students: { $sum: "$students.number" }
         }
      },
      { 
        $lookup: {
          from: "second",
          localField: "country",
          foreignField: "country",
          as: "second"
        },
      },
      {
        $set: {
          diff: { $subtract: ["$amount_students", { $first: "$second.overallStudents" }] },
        }
      },
      {
        $group: {
          _id: "$country",
          allDiffs: {
            $push: "$diff"
          },
          count: { $sum: 1 },
          longitude: {
            $push: "$longitude"
          },
          latitude: {
            $push: "$latitude"
          },
        }
      },
      {
        $out: "third"
      }
    ]).toArray();

    client.close();
  } catch (err) {
    console.error(err);
  }
})();