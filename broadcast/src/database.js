const { MongoClient } = require('mongodb');

let _db;
const getDatabase = async () => {
  if (_db) {
    return _db;
  }

  const mongoURL = process.env.MONGO_URL;

  const client = await MongoClient.connect(mongoURL, {
    useUnifiedTopology: true,
  });

  _db = client.db();
  return _db;
};

module.exports = getDatabase;
