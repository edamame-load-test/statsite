let data = '';

process.stdin.setEncoding('utf8');

process.stdin.on('readable', () => {
  let chunk = process.stdin.read();
  if (chunk !== null) data += chunk;
});

process.stdin.on('end', async () => {
  if (!data) return;

  const { Client } = require('pg');
  const client = new Client();

  await client.connect();

  const gauges = {};

  const rows = data.split("\n");
  console.log(rows);
  for (row of rows) {
    if (row === "") continue;

    const [key, value, timestamp] = row.split("|");
    const [metricType, testId, ...parts] = key.split(".");

    if (metricType === "gauges" && parts.length > 1) {
      if (!gauges[parts[1]]) {
        gauges[parts[1]] = {
          value: Number.parseFloat(value),
          timestamp,
          testId
        }
      } else {
        gauges[parts[1]].value += Number.parseFloat(value);
      }
    } else {
      const metric = [metricType, ...parts].join('.');
      await client.query('INSERT INTO samples (ts, metric, value, test_id) VALUES (to_timestamp($1), $2, $3, $4)', [timestamp, metric, value, testId]);
    }
  };

  for (let metric in gauges) {
    await client.query('INSERT INTO samples (ts, metric, value, test_id) VALUES (to_timestamp($1), $2, $3, $4)', [gauges[metric].timestamp, metric, gauges[metric].value, gauges[metric].testId]);
  }

  await client.end();
});