const sqlite = require("/app/node_modules/@louislam/sqlite3");

const database = new sqlite.Database("/app/data/kuma.db");
database.run(
  "INSERT INTO setting (key, value, type) VALUES ('disableAuth', 'true', 'security') " +
    "ON CONFLICT (key) DO UPDATE SET value = 'true'",
  (error) => {
    if (error) throw error;
    database.close();
  },
);
