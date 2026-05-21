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
 * 1. GET Handshake / Time Sync & Downstream Command Delivery Hook
 */
app.get('/iclock/cdata', async (req, res) => {
  // Normalize the query keys to handle any device firmware casing quirks (SN vs sn)
  const sn = (req.query.SN || req.query.sn || '').toUpperCase();
  
  console.log(`[Incoming GET Heartbeat] Query Params: ${JSON.stringify(req.query)}`);
  console.log(`[Device Heartbeat] Connection ping from Device SN: ${sn || 'Unknown'}`);

  // Construct localized SQL-safe timestamp format (YYYY-MM-DD HH:mm:ss)
  const now = new Date();
  const pad = (num) => String(num).padStart(2, '0');
  const serverTime = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
                     `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  // FIX: If we have any valid Serial Number checking in, check the queue immediately!
  if (sn) {
    try {
      // Find the oldest pending instruction targeted at this specific device SN
      const [rows] = await pool.query(
        "SELECT id, command_string FROM device_commands WHERE device_sn = ? AND status = 'pending' ORDER BY id ASC LIMIT 1",
        [sn]
      );

      if (rows.length > 0) {
        const cmd = rows[0];
        
        // Advance status to 'sent' so the machine doesn't pick up duplicates on quick retry loops
        await pool.query("UPDATE device_commands SET status = 'sent' WHERE id = ?", [cmd.id]);
        
        // Output using official ADMS Instruction framing syntax
        const commandPayload = `C:${cmd.id}:${cmd.command_string}\n`;
        console.log(`[Downstream Sync] Sent Command ID ${cmd.id} to Device ${sn}: ${cmd.command_string}`);
        
        res.setHeader('Content-Type', 'text/plain');
        return res.status(200).send(commandPayload);
      }
    } catch (err) {
      console.error(`[Command Queue Error] Failed fetching rows for machine ${sn}:`, err.message);
    }
  }

  // Fallback default: Return standard validation registration frame if queue is empty
  const responsePayload = `OK\nRegistryCode=1\nServerTime=${serverTime}\n`;
  res.setHeader('Content-Type', 'text/plain');
  return res.status(200).send(responsePayload);
});

/**
 * 2. POST Log Data Receiver & Upstream Synchronization Processing Hub
 */
app.post('/iclock/cdata', async (req, res) => {
  const sn = (req.query.SN || req.query.sn || '').toUpperCase();
  const table = req.query.table;
  const rawBody = req.body;

  // CRITICAL INTERCEPT HOOK:
  console.log(`\n==================================================`);
  console.log(`[RAW INCOMING DUMP] TABLE IDENTIFIER: ${table}`);
  console.log(`==================================================`);
  console.log(req.body); 
  console.log(`================== END OF DUMP ==================\n`);

  // --- BUCKET A: PROCESS TRANSACTIONS FOR ATTENDANCE RECORD SWIPES ---
  if (table === 'ATTLOG' && rawBody) {
    console.log(`[Data Push] Processing log package stack from terminal SN: ${sn || 'Unknown'}`);
    const lines = rawBody.split('\n');
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      for (let line of lines) {
        line = line.trim();
        if (!line) continue;

        const tokens = line.split('\t');
        if (tokens.length >= 2) {
          const userId = parseInt(tokens[0], 10);
          const timestamp = tokens[1];
          const statusCode = tokens[2] || 0; 
          const verifyType = tokens[3] || 0;

          // Integrity check: avoid foreign key faults if profile sync lags behind swipe logs
          const [empCheck] = await connection.query('SELECT employee_id FROM employees WHERE employee_id = ?', [userId]);
          if (empCheck.length === 0) {
            console.warn(`[Missing Profile] Auto-generating registration stub row for user ID ${userId}`);
            await connection.query(
              "INSERT INTO employees (employee_id, first_name, badge_number, department_id) VALUES (?, ?, ?, NULL)",
              [userId, `Device Enroll ${userId}`, `AUTO_SN_${sn}_${userId}`]
            );
          }

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

  // --- BUCKET B: UPSTREAM SYNC - USER CREATION ENROLLMENT CAPTURE ---
  if (table === 'user' && rawBody) {
    console.log(`[Upstream Sync] User profile pushed from physical keypad on Device: ${sn || 'Unknown'}`);
    const lines = rawBody.split('\n');
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();

      for (let line of lines) {
        line = line.trim();
        if (!line) continue;

        const tokens = line.split('\t');
        const pinToken = tokens.find(t => t.startsWith('Pin='));
        const nameToken = tokens.find(t => t.startsWith('Name='));

        if (pinToken) {
          const userId = parseInt(pinToken.split('=')[1], 10);
          const rawName = nameToken ? nameToken.split('=')[1] : `Keypad Enroll ${userId}`;

          const [exists] = await connection.query("SELECT employee_id FROM employees WHERE employee_id = ?", [userId]);
          if (exists.length === 0) {
            await connection.query(
              "INSERT INTO employees (employee_id, first_name, badge_number) VALUES (?, ?, ?)",
              [userId, rawName, `DEV_${sn}_${userId}`]
            );
            console.log(`[Upstream Sync] Created fresh employee registry entry for User: ${rawName}`);
          } else {
            await connection.query("UPDATE employees SET first_name = ? WHERE employee_id = ?", [rawName, userId]);
            console.log(`[Upstream Sync] Updated profile metadata name string for ID: ${userId}`);
          }
        }
      }
      await connection.commit();
    } catch (err) {
      await connection.rollback();
      console.error(`[Sync Failure] Processing User update failed:`, err.message);
    } finally {
      connection.release();
    }
  }

  // --- BUCKET C: SYNC CALLBACK ACCEPTS AND EXECUTED COMMAND CLOSURES ---
  // Some firmware sends command updates under table='options' or table='data'
  if ((table === 'options' || table === 'data') && rawBody) {
    const lines = rawBody.split('\n');
    for (let line of lines) {
      if (line.includes('Return=') || line.includes('ID=')) {
        const parts = line.split('&');
        const idPart = parts.find(p => p.startsWith('ID='));
        const retPart = parts.find(p => p.startsWith('Return='));
        
        if (idPart) {
          const cmdId = idPart.split('=')[1];
          // Default to 0 (Success) if return code isn't explicitly sent
          const returnCode = retPart ? parseInt(retPart.split('=')[1], 10) : 0;
          
          console.log(`[Command Callback] Device SN: ${sn} returned Code: ${returnCode} for Command ID: ${cmdId}`);
          
          if (returnCode >= 0) {
            await pool.query("UPDATE device_commands SET status = 'executed' WHERE id = ?", [cmdId]);
            console.log(`[Success] Command ID ${cmdId} updated to EXECUTED.`);
          } else {
            await pool.query("UPDATE device_commands SET status = 'pending' WHERE id = ?", [cmdId]);
          }
        }
      }
    }
  }

  // CRITICAL: Respond with an explicit text literal 'OK\n'
  res.setHeader('Content-Type', 'text/plain');
  return res.status(200).send('OK\n');
});

app.listen(port, () => {
  console.log(`Single-Tenant Cloud ADMS listener running actively on port ${port}`);
  initDb();
});