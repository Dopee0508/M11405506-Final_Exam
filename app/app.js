const express = require('express');
const app = express();
const port = 3000;

app.get('/', (req, res) => {
  res.send('<h1>DADD Exam Project is Running!</h1><p>Database ETL should be complete.</p>');
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});