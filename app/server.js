const express = require('express');
const mysql = require('mysql2/promise');

const app = express();
const port = 8081;

// Single-Tenant Host Database Mapping Profiles
const dbConfig = {
  host: 'localhost',
  user: 'edwin',
  password: 'NAMA1234',
  database: 'zk_attendance',
  waitForConnections: true,
  connectionLimit: 15,
  queueLimit: 0
};

let pool;

async function initDb() {
  try {
    pool = mysql.createPool(dbConfig);
    console.log("MySQL connection pool successfully attached to host loopback.");
  } catch (err) {
    console.error("Database connection failed. Retrying in 5 seconds...", err.message);
    setTimeout(initDb, 5000);
  }
}

// Accept raw multi-line strings sent via text/plain payloads by firmware controller
app.use(express.text({ type: '*/*', limit: '10mb' }));

/**
 * 1. GET Handshake / Time Sync Hook
 */
app.get('/iclock/cdata', (req, res) => {
  const sn = req.query.SN;
  console.log(`[Handshake Ping] Inbound connection established from Device SN: ${sn || 'Unknown'}`);

  // Construct localized SQL-safe timestamp format (YYYY-MM-DD HH:mm:ss)
  const now = new Date();
  const pad = (num) => String(num).padStart(2, '0');
  const serverTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
                     `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  const responsePayload = `OK\nRegistryCode=1\nServerTime=${serverTime}\n`;
  
  res.setHeader('Content-Type', 'text/plain');
  return res.status(200).send(responsePayload);
});

/**
 * 2. POST Log Data Receiver
 */
app.post('/iclock/cdata', async (req, res) => {
  const sn = req.query.SN;
  const table = req.query.table;
  const rawBody = req.body;

  console.log(`[Data Push] Processing log package stack from terminal SN: ${sn || 'Unknown'}`);

  if (table === 'ATTLOG' && rawBody) {
    const lines = rawBody.split('\n');
    
    // Begin a heavy transaction execution block to speed up bulk log writes safely
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      for (let line of lines) {
        line = line.trim();
        if (!line) continue;

        const tokens = line.split('\t');
        if (tokens.length >= 2) {
          const userId = tokens[0];
          const timestamp = tokens[1];
          const statusCode = tokens[2] || 0; 
          const verifyType = tokens[3] || 0;

          // Push into the central logs data vault
          await connection.query(
            `INSERT INTO attendance_logs (device_sn, user_id, timestamp, status_code, verify_type) 
             VALUES (?, ?, ?, ?, ?)`,
            [sn, userId, timestamp, statusCode, verifyType]
          );
        }
      }

      await connection.commit();
      console.log(`[Success] Safely committed batch payload lines array to database storage profiles.`);
    } catch (txErr) {
      await connection.rollback();
      console.error(`[Error] Database insertion error routine triggered. Rollback executed:`, txErr.message);
    } finally {
      connection.release();
    }
  }

  // CRITICAL: Respond with an explicit text literal 'OK\n' or the device 
  // will loop the same data package endlessly thinking the transaction dropped out.
  res.setHeader('Content-Type', 'text/plain');
  return res.status(200).send('OK\n');
});

app.listen(port, () => {
  console.log(`Single-Tenant Cloud ADMS listener running actively on port ${port}`);
  initDb();
});