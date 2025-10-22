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
  let dataBuffer = Buffer.alloc(0);
  
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
    dataBuffer = Buffer.alloc(0);
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
  
  // Process incoming data
  socket.on('data', async (chunk) => {
    try {
      const session = activeSessions.get(sessionId);
      if (!session) return;
      
      // Append chunk to buffer
      dataBuffer = Buffer.concat([dataBuffer, chunk]);
      
      // Update session stats
      session.lastDataAt = new Date();
      session.bytesReceived += chunk.length;
      
      // Limit total buffer size
      if (dataBuffer.length > 64 * 1024) {
        console.error(`[GPS] ❌ Buffer too large from ${sessionId}: ${dataBuffer.length} bytes`);
        socket.destroy();
        return;
      }
      
      console.log(`[GPS] 📦 Received ${chunk.length} bytes from ${sessionId} (buffer: ${dataBuffer.length} bytes)`);
      
      // Process frames
      while (dataBuffer.length > 0) {
        let parsed = null;
        let bytesConsumed = 0;
        
        // 1. IMEI Authentication (17 bytes: 2 length + 15 IMEI)
        if (!authenticatedIMEI && dataBuffer.length >= 17) {
          const imeiLength = dataBuffer.readUInt16BE(0);
          if (imeiLength === 15) {
            const imeiString = dataBuffer.slice(2, 17).toString('ascii');
            
            session.imei = imeiString;
            authenticatedIMEI = imeiString;
            session.packetsReceived++;
            
            console.log(`[GPS] 🔐 Authenticated: ${sessionId} | IMEI: ${imeiString}`);
            
            // Send IMEI acceptance (0x01)
            socket.write(Buffer.from([0x01]));
            
            // Consume IMEI packet
            dataBuffer = dataBuffer.slice(17);
            continue;
          }
        }
        
        // 2. AVL Data (after authentication)
        if (authenticatedIMEI && dataBuffer.length >= 8) {
          // Check for valid AVL packet (4 zeros + length)
          const preamble = dataBuffer.readUInt32BE(0);
          if (preamble !== 0) {
            console.error(`[GPS] ❌ Invalid AVL preamble: ${preamble.toString(16)}`);
            dataBuffer = Buffer.alloc(0);
            break;
          }
          
          const dataLength = dataBuffer.readUInt32BE(4);
          const totalPacketSize = 8 + dataLength + 4; // preamble + length + data + CRC
          
          console.log(`[GPS] 📏 AVL packet - Data length: ${dataLength}, Total: ${totalPacketSize} bytes, Buffer: ${dataBuffer.length}`);
          
          // Wait for complete packet
          if (dataBuffer.length < totalPacketSize) {
            console.log(`[GPS] ⏳ Waiting for complete packet (need ${totalPacketSize}, have ${dataBuffer.length})`);
            break;
          }
          
          // Parse AVL data
          try {
            const parser = new ProtocolParser(dataBuffer.toString('hex'));
            
            if (parser.Content && parser.Content.AVL_Datas && parser.Content.AVL_Datas.length > 0) {
              const rawRecords = parser.Content.AVL_Datas;
              console.log(`[GPS] 📍 Parsed ${rawRecords.length} GPS records from IMEI: ${authenticatedIMEI}`);
              
              // Format records for Fleetee API
              const records = rawRecords.map(record => ({
                timestamp: new Date(record.Timestamp).getTime(),
                latitude: record.GPSelement?.Latitude || 0,
                longitude: record.GPSelement?.Longitude || 0,
                speed: record.GPSelement?.Speed || 0,
                angle: record.GPSelement?.Angle || null,
                altitude: record.GPSelement?.Altitude || null,
                satellites: record.GPSelement?.Satellites || null,
                odometer: record.IOelement?.Elements?.[199] || null,
                ignition: record.IOelement?.Elements?.[239] === 1 ? true : (record.IOelement?.Elements?.[239] === 0 ? false : null),
              }));
              
              // Forward to Fleetee API (non-blocking)
              setImmediate(async () => {
                await forwardToFleetee(authenticatedIMEI, records);
              });
              
              // Send acknowledgment (4 bytes: number of records)
              const ackBuffer = Buffer.alloc(4);
              ackBuffer.writeUInt32BE(rawRecords.length, 0);
              socket.write(ackBuffer);
              
              console.log(`[GPS] ✅ Acknowledged ${rawRecords.length} records to device`);
              
              session.packetsReceived++;
              bytesConsumed = totalPacketSize;
            } else {
              console.warn(`[GPS] ⚠️  Parser returned no AVL data`);
              bytesConsumed = totalPacketSize; // Consume anyway
            }
          } catch (parseError) {
            console.error(`[GPS] ❌ Parser error:`, parseError.message);
            // Consume the packet anyway to avoid infinite loop
            bytesConsumed = totalPacketSize;
          }
          
          // Remove consumed bytes
          if (bytesConsumed > 0) {
            dataBuffer = dataBuffer.slice(bytesConsumed);
          } else {
            // Safety: clear buffer if we couldn't parse
            console.warn(`[GPS] ⚠️  Clearing buffer due to parse failure`);
            dataBuffer = Buffer.alloc(0);
            break;
          }
        } else {
          // Not enough data for AVL packet
          break;
        }
      } // End while loop
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

// Health check server
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
