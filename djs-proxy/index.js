const http = require('http');
const https = require('https');
const { URL } = require('url');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  // 1. REST Proxy
  let path = req.url;
  if (path.startsWith('/api')) {
    path = path.slice(4);
  }

  const targetUrl = `https://discord.com/api${path}`;
  const parsedUrl = new URL(targetUrl);

  const headers = { ...req.headers };
  delete headers.host;
  delete headers['cf-connecting-ip'];
  delete headers['x-real-ip'];

  console.log(`[REST] ${req.method} ${targetUrl}`);

  const proxyReq = https.request({
    hostname: parsedUrl.hostname,
    port: 443,
    path: parsedUrl.pathname + parsedUrl.search,
    method: req.method,
    headers: headers
  }, (proxyRes) => {
    // If it's /gateway/bot, rewrite the WebSocket URL to force client through this proxy
    if (path.endsWith('/gateway/bot') && proxyRes.statusCode === 200) {
      let body = '';
      proxyRes.on('data', (chunk) => { body += chunk; });
      proxyRes.on('end', () => {
        try {
          const data = JSON.parse(body);
          const host = req.headers.host;
          
          // Force discord.js to use this proxy server for WebSocket connection
          data.url = `ws://${host}`;
          
          const responseBody = JSON.stringify(data);
          const resHeaders = { ...proxyRes.headers };
          resHeaders['content-length'] = Buffer.byteLength(responseBody);
          res.writeHead(proxyRes.statusCode, resHeaders);
          res.end(responseBody);
        } catch (e) {
          res.writeHead(proxyRes.statusCode, proxyRes.headers);
          res.end(body);
        }
      });
    } else {
      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res);
    }
  });

  proxyReq.on('error', (err) => {
    console.error(`[REST Error] ${err.message}`);
    res.writeHead(502);
    res.end(`Proxy REST Error: ${err.message}`);
  });

  req.pipe(proxyReq);
});

// 2. WebSocket Gateway Proxy
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const targetUrl = `wss://gateway.discord.gg${url.pathname}${url.search}`;

  console.log(`[WS] Connecting to Discord Gateway: ${targetUrl}`);

  const targetHeaders = { ...request.headers };
  delete targetHeaders.host;

  const targetWs = new WebSocket(targetUrl, {
    headers: targetHeaders
  });

  wss.handleUpgrade(request, socket, head, (clientWs) => {
    // Pipe client <-> target WebSocket traffic
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
