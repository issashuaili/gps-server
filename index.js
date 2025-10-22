#!/usr/bin/env node

/**
 * Fleetee GPS TCP Server
 * 
 * This standalone server accepts TCP connections from Teltonika GPS devices,
 * parses the binary Codec 8/8E protocol, and forwards the data to your
 * Fleetee application via HTTP POST.
 * 
 * Environment Variables Required:
 * - FLEETEE_API_URL: Your Fleetee app URL (e.g., https://fleet-manager-saa-s-alshuaili911.replit.app)
 * - GPS_SERVER_SECRET: Shared secret for API authentication
 * - TCP_PORT: Port to listen on (default: 5000)
 */

require('dotenv').config();
const net = require('net');
const { ProtocolParser } = require('complete-teltonika-parser');

// Configuration
const TCP_PORT = process.env.TCP_PORT || 5000;
const FLEETEE_API_URL = process.env.FLEETEE_API_URL;
const GPS_SERVER_SECRET = process.env.GPS_SERVER_SECRET;

if (!FLEETEE_API_URL) {
  console.error('ERROR: FLEETEE_API_URL environment variable is required!');
  console.error('Example: FLEETEE_API_URL=https://your-app.replit.app');
  process.exit(1);
}

if (!GPS_SERVER_SECRET) {
  console.error('ERROR: GPS_SERVER_SECRET environment variable is required!');
  console.error('This should match the GPS_SERVER_SECRET in your Fleetee app');
  process.exit(1);
}

// Session tracking
const activeSessions = new Map();

// Forward GPS data to Fleetee API
async function forwardToFleetee(imei, records) {
  try {
    const response = await fetch(`${FLEETEE_API_URL}/api/gps/ingest`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${GPS_SERVER_SECRET}`
      },
      body: JSON.stringify({ imei, records })
    });

    const result = await response.json();
    
    if (!response.ok) {
      console.error(`[API] Error from Fleetee: ${response.status}`, result);
      return { success: false, accepted: 0 };
    }

    console.log(`[API] Fleetee accepted ${result.accepted}/${result.total} records for IMEI: ${imei}`);
    return { success: true, accepted: result.accepted };
  } catch (error) {
    console.error(`[API] Failed to forward data to Fleetee:`, error.message);
    return { success: false, accepted: 0 };
  }
}

// Handle TCP connection from GPS device
function handleGpsConnection(socket) {
  const sessionId = `${socket.remoteAddress}:${socket.remotePort}`;
  let authenticatedIMEI = null;
  let dataBuffer = Buffer.alloc(0); // Buffer for accumulating TCP stream data
  
  // Create session
  activeSessions.set(sessionId, {
    imei: null,
    connectedAt: new Date(),
    lastDataAt: new Date(),
    packetsReceived: 0,
    bytesReceived: 0,
  });
  
  console.log(`\n[GPS] ✅ New connection: ${sessionId}`);
  
  // Cleanup on disconnect
  const cleanup = () => {
    const session = activeSessions.get(sessionId);
    if (session) {
      console.log(`[GPS] ❌ Disconnected: ${sessionId} | IMEI: ${session.imei || 'none'} | Packets: ${session.packetsReceived}`);
      activeSessions.delete(sessionId);
    }
    dataBuffer = Buffer.alloc(0); // Clear buffer
  };
  
  socket.on('end', cleanup);
  socket.on('close', cleanup);
  socket.on('error', (error) => {
    console.error(`[GPS] Socket error for ${sessionId}:`, error.message);
    cleanup();
  });
  
  // 5-minute idle timeout
  socket.setTimeout(5 * 60 * 1000);
  socket.on('timeout', () => {
    console.log(`[GPS] ⏱️  Timeout: ${sessionId}`);
    socket.destroy();
  });
  
  // Process incoming data with proper buffering for TCP stream
  socket.on('data', async (chunk) => {
    try {
      const session = activeSessions.get(sessionId);
      if (!session) return;
      
      // Append chunk to buffer
      dataBuffer = Buffer.concat([dataBuffer, chunk]);
      
      // Update session stats
      session.lastDataAt = new Date();
      session.bytesReceived += chunk.length;
      
      // Limit total buffer size to prevent memory issues
      if (dataBuffer.length > 64 * 1024) {
        console.error(`[GPS] ❌ Buffer too large from ${sessionId}: ${dataBuffer.length} bytes`);
        socket.destroy();
        return;
      }
      
      console.log(`[GPS] 📦 Received ${chunk.length} bytes from ${sessionId} (buffer: ${dataBuffer.length} bytes)`);
      
      // Process all complete frames in the buffer
      // Teltonika devices often send multiple frames in one TCP chunk
      while (dataBuffer.length > 0) {
        let parsed;
        let frameConsumed = false;
        
        try {
          const parser = new ProtocolParser(dataBuffer.toString('hex'));
          parsed = {
            imei: parser.imei || null,
            avl: parser.avl || null,
          };
          frameConsumed = true;
        } catch (parseError) {
          // Parser failed - either incomplete frame or bad data
          // If we have a reasonable amount of data but still can't parse, log warning
          if (dataBuffer.length > 512) {
            console.warn(`[GPS] ⚠️  Waiting for more data from ${sessionId} (buffer: ${dataBuffer.length} bytes)`);
          }
          break; // Exit loop, wait for more data
        }
        
        if (!frameConsumed) break;
        
        // Calculate consumed bytes (IMEI packet is 17 bytes: 2 preamble + 15 IMEI)
        // AVL packet structure varies, we'll estimate conservatively
        let bytesConsumed = 0;
        
        if (parsed.imei && !parsed.avl) {
          // IMEI packet: 2 bytes (length) + 15 bytes (IMEI)
          bytesConsumed = 17;
        } else if (parsed.avl) {
          // AVL packet: read data length from buffer (bytes 4-7)
          if (dataBuffer.length >= 8) {
            const dataLength = dataBuffer.readUInt32BE(4);
            bytesConsumed = 8 + dataLength + 4; // preamble(4) + length(4) + data + CRC(4)
          }
        }
        
        // If we couldn't determine bytes consumed, assume entire buffer (conservative)
        if (bytesConsumed === 0 || bytesConsumed > dataBuffer.length) {
          console.warn(`[GPS] ⚠️  Could not determine frame size, clearing buffer`);
          dataBuffer = Buffer.alloc(0);
          break;
        }
        
        // Remove consumed bytes from buffer
        dataBuffer = dataBuffer.slice(bytesConsumed);
        session.packetsReceived++;
        
        // Handle IMEI authentication
        if (parsed.imei && !session.imei) {
          session.imei = parsed.imei;
          authenticatedIMEI = parsed.imei;
          console.log(`[GPS] 🔐 Authenticated: ${sessionId} | IMEI: ${parsed.imei}`);
          
          // Send IMEI acceptance (1 byte: 0x01)
          socket.write(Buffer.from([0x01]));
          continue; // Process next frame in buffer
        }
        
        // Process AVL data (GPS records)
        if (parsed.avl && parsed.avl.data && parsed.avl.data.length > 0) {
          const imei = session.imei || authenticatedIMEI;
          if (!imei) {
            console.warn(`[GPS] ⚠️  Received AVL data without IMEI from ${sessionId}`);
            continue; // Skip this frame
          }
          
          const rawRecords = parsed.avl.data;
          console.log(`[GPS] 📍 Processing ${rawRecords.length} GPS records from IMEI: ${imei}`);
          
          // Format records for Fleetee API
          const records = rawRecords.map(record => ({
            timestamp: record.timestampMs || Date.now(),
            latitude: record.latitude || 0,
            longitude: record.longitude || 0,
            speed: record.speed || null,
            angle: record.angle || null,
            odometer: record.ioElements?.find(io => io.id === 199)?.value || null,
            ignition: record.ioElements?.find(io => io.id === 239)?.value === 1 ? true : null,
          }));
          
          // Forward to Fleetee API (non-blocking)
          setImmediate(async () => {
            await forwardToFleetee(imei, records);
          });
          
          // Send acknowledgment to device (number of records processed)
          const recordCount = rawRecords.length;
          const ackBuffer = Buffer.alloc(4);
          ackBuffer.writeUInt32BE(recordCount, 0);
          socket.write(ackBuffer);
          
          console.log(`[GPS] ✅ Acknowledged ${recordCount} records to device`);
        }
      } // End while loop - process next frame in buffer
    } catch (error) {
      console.error(`[GPS] ❌ Error processing data from ${sessionId}:`, error);
    }
  });
}

// Create TCP server
const server = net.createServer(handleGpsConnection);

server.listen(TCP_PORT, '0.0.0.0', () => {
  console.log('\n========================================');
  console.log('🚀 Fleetee GPS TCP Server Started');
  console.log('========================================');
  console.log(`📡 Listening on: 0.0.0.0:${TCP_PORT}`);
  console.log(`🔗 Forwarding to: ${FLEETEE_API_URL}`);
  console.log(`🔐 Auth secret: ${GPS_SERVER_SECRET.substring(0, 8)}...`);
  console.log('========================================\n');
  console.log('Waiting for GPS devices to connect...\n');
});

server.on('error', (error) => {
  console.error('❌ Server error:', error);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('\n⏹️  Received SIGTERM, shutting down gracefully...');
  server.close(() => {
    console.log('✅ Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('\n⏹️  Received SIGINT, shutting down gracefully...');
  server.close(() => {
    console.log('✅ Server closed');
    process.exit(0);
  });
});

// Status endpoint (optional - for health checks)
const http = require('http');
const statusPort = process.env.STATUS_PORT || 3000;

const statusServer = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    const stats = {
      status: 'running',
      uptime: process.uptime(),
      activeSessions: activeSessions.size,
      sessions: Array.from(activeSessions.entries()).map(([id, session]) => ({
        id,
        imei: session.imei,
        connectedAt: session.connectedAt,
        packetsReceived: session.packetsReceived,
      })),
    };
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(stats, null, 2));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

statusServer.listen(statusPort, () => {
  console.log(`📊 Health check server running on port ${statusPort}`);
  console.log(`   Visit http://localhost:${statusPort}/health for status\n`);
});
