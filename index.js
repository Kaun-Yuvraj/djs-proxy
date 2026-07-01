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

  console.log(`[WS] Connecting to Discord Gateway: ${targetUrl}`);

  const targetHeaders = {};
  for (const [key, value] of Object.entries(request.headers)) {
    if (key !== 'host' && key !== 'connection' && key !== 'upgrade') {
      targetHeaders[key] = value;
    }
  }

  const targetWs = new WebSocket(targetUrl, {
    headers: targetHeaders
  });

  wss.handleUpgrade(request, socket, head, (clientWs) => {
    clientWs.on('message', (message) => {
      if (targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(message);
      }
    });

    clientWs.on('close', (code, reason) => {
      targetWs.close(code, reason);
    });

    clientWs.on('error', (err) => {
      console.error('[WS Client Error]', err.message);
      targetWs.close();
    });

    targetWs.on('message', (message) => {
      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.send(message);
      }
    });

    targetWs.on('close', (code, reason) => {
      clientWs.close(code, reason);
    });

    targetWs.on('error', (err) => {
      console.error('[WS Gateway Error]', err.message);
      clientWs.close();
    });
  });
});

server.listen(PORT, () => {
  console.log(`Discord proxy server listening on port ${PORT}`);
});
