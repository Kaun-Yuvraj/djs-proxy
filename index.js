const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer(async (req, res) => {
  // 1. REST Proxy
  let path = req.url;
  if (path.startsWith('/api')) {
    path = path.slice(4);
  }

  const discordApiUrl = `https://discord.com/api${path}`;
  
  // Clone and clean request headers
  const headers = {};
  for (const [key, value] of Object.entries(req.headers)) {
    if (key !== 'host' && key !== 'connection' && key !== 'keep-alive') {
      headers[key] = value;
    }
  }

  console.log(`[REST] ${req.method} ${discordApiUrl}`);

  try {
    const fetchOptions = {
      method: req.method,
      headers: headers,
    };

    // Forward request body if applicable
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      const buffers = [];
      for await (const chunk of req) {
        buffers.push(chunk);
      }
      fetchOptions.body = Buffer.concat(buffers);
      fetchOptions.duplex = 'half';
    }

    const response = await fetch(discordApiUrl, fetchOptions);

    // If it's /gateway/bot, rewrite the WebSocket URL
    if (path.endsWith('/gateway/bot') && response.status === 200) {
      const data = await response.json();
      const host = req.headers.host;
      
      // Force discord.js to use this proxy for the WS connection
      data.url = `wss://${host}`;
      
      const responseBody = JSON.stringify(data);
      res.writeHead(200, {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(responseBody)
      });
      return res.end(responseBody);
    }

    // Forward the response headers and status (cleaning up compression/length headers)
    const resHeaders = {};
    response.headers.forEach((value, key) => {
      const lowerKey = key.toLowerCase();
      // Since fetch decompresses the response automatically, we must NOT send
      // content-encoding/content-length/transfer-encoding headers to the client.
      if (
        lowerKey !== 'content-encoding' &&
        lowerKey !== 'transfer-encoding' &&
        lowerKey !== 'content-length'
      ) {
        resHeaders[key] = value;
      }
    });

    res.writeHead(response.status, resHeaders);
    
    // Pipe the response body stream to the client
    if (response.body) {
      const reader = response.body.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        res.write(value);
      }
    }
    res.end();

  } catch (err) {
    console.error(`[REST Error] ${err.message}`);
    res.writeHead(502);
    res.end(`Proxy REST Error: ${err.message}`);
  }
});

// 2. WebSocket Gateway Proxy
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const targetUrl = `wss://gateway.discord.gg${url.pathname}${url.search}`;

  console.log(`[WS] Incoming upgrade. Connecting to Discord: ${targetUrl}`);

  const targetHeaders = {};
  for (const [key, value] of Object.entries(request.headers)) {
    if (key !== 'host' && key !== 'connection' && key !== 'upgrade') {
      targetHeaders[key] = value;
    }
  }

  const targetWs = new WebSocket(targetUrl, {
    headers: targetHeaders
  });

  const targetQueue = [];
  const clientQueue = [];
  let clientWs = null;
  let targetOpen = false;

  // Listeners on Target (Discord) WebSocket
  targetWs.on('open', () => {
    console.log('[WS] Connected to Discord Gateway successfully');
    targetOpen = true;
    // Flush client queue
    while (clientQueue.length > 0) {
      const { message, isBinary } = clientQueue.shift();
      if (targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(message, { binary: isBinary });
      }
    }
  });

  targetWs.on('message', (message, isBinary) => {
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(message, { binary: isBinary });
    } else {
      targetQueue.push({ message, isBinary });
    }
  });

  targetWs.on('close', (code, reason) => {
    console.log(`[WS] Discord Gateway closed: ${code} - ${reason.toString()}`);
    if (clientWs) clientWs.close(code, reason);
  });

  targetWs.on('error', (err) => {
    console.error('[WS] Discord Gateway error:', err.message);
    if (clientWs) clientWs.close(1011, err.message);
  });

  // Upgrade the incoming connection to Client WebSocket
  wss.handleUpgrade(request, socket, head, (ws) => {
    clientWs = ws;
    console.log('[WS] Client connection upgraded successfully');

    // Flush target queue (delivers early packets like Hello from Discord)
    while (targetQueue.length > 0) {
      const { message, isBinary } = targetQueue.shift();
      clientWs.send(message, { binary: isBinary });
    }

    clientWs.on('message', (message, isBinary) => {
      if (targetOpen && targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(message, { binary: isBinary });
      } else {
        clientQueue.push({ message, isBinary });
      }
    });

    clientWs.on('close', (code, reason) => {
      console.log(`[WS] Client closed connection: ${code} - ${reason.toString()}`);
      targetWs.close(code, reason);
    });

    clientWs.on('error', (err) => {
      console.error('[WS] Client connection error:', err.message);
      targetWs.close(1011, err.message);
    });
  });
});

server.listen(PORT, () => {
  console.log(`Discord proxy server listening on port ${PORT}`);
});
