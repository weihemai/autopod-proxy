/**
 * Autopod proxy — Render.com edition
 * ---------------------------------------------------------------
 * A static app (GitHub Pages) can't reliably call the iTunes Search
 * API or most podcast RSS feeds directly from the browser: iTunes
 * sends no CORS headers at all, and most independent podcast feeds
 * don't either. This tiny proxy fetches both server-side (where CORS
 * doesn't apply) and relays the result with permissive CORS headers.
 *
 * Routes:
 *   GET /search?q=<term>          -> proxies itunes.apple.com/search
 *   GET /feed?url=<feed url>      -> proxies an arbitrary RSS/Atom feed
 *   GET /stream?url=<audio url>   -> pipes an audio stream through (China
 *                                    Great-Firewall fallback only — see
 *                                    below). Requires HTTP Basic Auth.
 *
 * No external dependencies — pure Node.js (`http`/`https` only).
 */

const http = require('http');
const https = require('https');

const PORT = process.env.PORT || 3000;
const REQUEST_TIMEOUT_MS = 10000;

// ---- /stream protection: this route pipes arbitrary audio bytes and is a
// much more attractive abuse target (free bandwidth/IP-cloaking) than the
// JSON/XML routes above, so it requires Basic Auth, restricts CORS to known
// app origins, and rate-limits per IP. Credentials are never committed to
// source — set them as env vars on Render and enter the same values in each
// app's Settings screen (stored only in that browser's localStorage).
const STREAM_PROXY_USER = process.env.STREAM_PROXY_USER || '';
const STREAM_PROXY_PASS = process.env.STREAM_PROXY_PASS || '';
const ALLOWED_STREAM_ORIGINS = ['https://weihemai.github.io'];
const STREAM_RATE_LIMIT_PER_MIN = 30;

function corsHeaders(extra={}){
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Range, Authorization',
    'Access-Control-Expose-Headers': 'Content-Range, Accept-Ranges, Content-Length',
    'Cache-Control': 'no-store',
    ...extra
  };
}

function sendJson(res, status, obj){
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', ...corsHeaders() });
  res.end(body);
}

function fetchUrl(targetUrl){
  return new Promise((resolve, reject) => {
    const lib = targetUrl.startsWith('https:') ? https : http;
    const req = lib.get(targetUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AutopodProxy/1.0)' },
      timeout: REQUEST_TIMEOUT_MS
    }, (res) => {
      // Follow a single redirect hop (common for feed URLs).
      if(res.statusCode >= 300 && res.statusCode < 400 && res.headers.location){
        res.resume();
        return fetchUrl(new URL(res.headers.location, targetUrl).toString()).then(resolve, reject);
      }
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, contentType: res.headers['content-type'] || '' }));
    });
    req.on('timeout', () => req.destroy(new Error('Upstream request timed out')));
    req.on('error', reject);
  });
}

// ---- /stream: pipes audio through instead of buffering it (episodes can
// be hours long), forwards Range for seek support, and never applies a
// timeout to the piping itself — only to getting the upstream connection.
let streamBytesThisMonth = 0;
let streamBytesMonthKey = new Date().getMonth();
function trackStreamBytes(n){
  const m = new Date().getMonth();
  if(m !== streamBytesMonthKey){ streamBytesMonthKey = m; streamBytesThisMonth = 0; }
  streamBytesThisMonth += n;
}

const streamRateLimits = new Map(); // ip -> { count, windowStart }
function isStreamRateLimited(ip){
  const now = Date.now();
  const entry = streamRateLimits.get(ip);
  if(!entry || now - entry.windowStart > 60000){
    streamRateLimits.set(ip, { count: 1, windowStart: now });
    return false;
  }
  entry.count++;
  return entry.count > STREAM_RATE_LIMIT_PER_MIN;
}

function checkStreamAuth(req, reqUrl){
  if(!STREAM_PROXY_USER || !STREAM_PROXY_PASS) return false; // fail closed if unconfigured

  // Primary path: a real Authorization header (used by fetch()-based
  // callers like the Settings "Test connection" button).
  const [scheme, encoded] = (req.headers.authorization || '').split(' ');
  if(scheme === 'Basic' && encoded){
    let decoded;
    try{ decoded = Buffer.from(encoded, 'base64').toString('utf8'); }catch{ decoded = ''; }
    const sep = decoded.indexOf(':');
    if(sep !== -1 && decoded.slice(0, sep) === STREAM_PROXY_USER && decoded.slice(sep + 1) === STREAM_PROXY_PASS){
      return true;
    }
  }

  // Fallback path: plain ?user=&pass= query params. A native <audio src>
  // load can't set custom headers, and relies on the browser converting
  // userinfo embedded in the URL (https://user:pass@host/...) into a real
  // Authorization header — which turns out to be unreliable in at least
  // one real in-car WebView (confirmed: no Authorization header arrives,
  // no way to distinguish that from wrong credentials from here). Query
  // params have no such browser-dependent behavior, so this is what the
  // client actually uses for playback; the header path above stays for
  // fetch()-based calls that can set headers directly.
  if(reqUrl){
    const user = reqUrl.searchParams.get('user');
    const pass = reqUrl.searchParams.get('pass');
    if(user === STREAM_PROXY_USER && pass === STREAM_PROXY_PASS) return true;
  }

  return false;
}

function pipeStream(streamUrl, req, res, streamCors){
  const lib = streamUrl.startsWith('https:') ? https : http;
  const upstreamReq = lib.request(streamUrl, {
    // Each stream is its own long-lived, high-bandwidth connection — not a
    // good candidate for Node's default keep-alive pooling, which was
    // observed to desync ("Parse Error: Expected HTTP/...") when a socket
    // got reused across overlapping stream requests.
    agent: false,
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; AutopodProxy/1.0)',
      ...(req.headers.range ? { Range: req.headers.range } : {})
    }
  }, (upstreamRes) => {
    // Follows redirects recursively (each hop re-enters this same
    // function), not just one — unlike fetchUrl() above, which really is
    // single-hop-only.
    if(upstreamRes.statusCode >= 300 && upstreamRes.statusCode < 400 && upstreamRes.headers.location){
      upstreamRes.resume();
      return pipeStream(new URL(upstreamRes.headers.location, streamUrl).toString(), req, res, streamCors);
    }
    if(upstreamRes.statusCode >= 400){
      // Not a network-level failure — the upstream host itself rejected
      // the request (e.g. anti-bot/hotlink protection blocking Render's
      // IP, or the file genuinely being gone). Log it since this is
      // otherwise invisible: the client just sees its <audio> load fail.
      console.log(`/stream upstream error ${upstreamRes.statusCode} for ${streamUrl}`);
    }
    res.writeHead(upstreamRes.statusCode, {
      'Content-Type': upstreamRes.headers['content-type'] || 'audio/mpeg',
      ...(upstreamRes.headers['content-length'] ? { 'Content-Length': upstreamRes.headers['content-length'] } : {}),
      ...(upstreamRes.headers['content-range'] ? { 'Content-Range': upstreamRes.headers['content-range'] } : {}),
      ...(upstreamRes.headers['accept-ranges'] ? { 'Accept-Ranges': upstreamRes.headers['accept-ranges'] } : {}),
      ...streamCors
    });
    let bytes = 0;
    upstreamRes.on('data', chunk => { bytes += chunk.length; trackStreamBytes(chunk.length); });
    upstreamRes.on('end', () => console.log(`/stream ${streamUrl} served ${bytes}B (month total: ${streamBytesThisMonth}B)`));
    upstreamRes.pipe(res);
  });
  // Only bound the time to get a response; once piping starts, let it run
  // for the life of playback — never apply a timeout to the pipe itself.
  upstreamReq.setTimeout(REQUEST_TIMEOUT_MS, () => upstreamReq.destroy(new Error('Upstream connection timed out')));
  upstreamReq.on('error', (e) => {
    // Only log/report this as a real failure if we hadn't already served a
    // response — some sources (e.g. Icecast) log a harmless parse
    // complaint on the leftover socket after a short Range request has
    // already been satisfied and torn down, which isn't an actual failure.
    if(!res.headersSent){
      console.log(`/stream upstream connection error for ${streamUrl}: ${e && e.message || e}`);
      sendJson(res, 502, { error: String(e && e.message || e) });
    }
  });
  req.on('close', () => upstreamReq.destroy());
  upstreamReq.end();
}

const server = http.createServer(async (req, res) => {
  if(req.method === 'OPTIONS'){
    res.writeHead(204, corsHeaders());
    return res.end();
  }

  const reqUrl = new URL(req.url, `http://${req.headers.host}`);

  if(reqUrl.pathname === '/search'){
    const q = reqUrl.searchParams.get('q');
    if(!q) return sendJson(res, 400, { error: 'Missing required "q" query parameter' });
    try{
      const upstream = 'https://itunes.apple.com/search?media=podcast&limit=15&term=' + encodeURIComponent(q);
      const result = await fetchUrl(upstream);
      res.writeHead(result.status, { 'Content-Type': 'application/json', ...corsHeaders() });
      res.end(result.body);
    }catch(e){
      sendJson(res, 502, { error: String(e && e.message || e) });
    }
    return;
  }

  if(reqUrl.pathname === '/feed'){
    const feedUrl = reqUrl.searchParams.get('url');
    if(!feedUrl) return sendJson(res, 400, { error: 'Missing required "url" query parameter' });
    let parsed;
    try{ parsed = new URL(feedUrl); }catch{ return sendJson(res, 400, { error: 'Invalid feed URL' }); }
    if(parsed.protocol !== 'http:' && parsed.protocol !== 'https:'){
      return sendJson(res, 400, { error: 'Only http(s) feed URLs are supported' });
    }
    try{
      const result = await fetchUrl(feedUrl);
      res.writeHead(result.status, { 'Content-Type': 'application/xml; charset=utf-8', ...corsHeaders() });
      res.end(result.body);
    }catch(e){
      sendJson(res, 502, { error: String(e && e.message || e) });
    }
    return;
  }

  if(reqUrl.pathname === '/stream'){
    const origin = req.headers.origin;
    const allowedOrigin = ALLOWED_STREAM_ORIGINS.includes(origin) ? origin : ALLOWED_STREAM_ORIGINS[0];
    const streamCors = corsHeaders({ 'Access-Control-Allow-Origin': allowedOrigin });

    if(origin && !ALLOWED_STREAM_ORIGINS.includes(origin)){
      console.log(`/stream 403 origin blocked: ${origin}`);
      res.writeHead(403, streamCors);
      return res.end();
    }
    if(!checkStreamAuth(req, reqUrl)){
      // Deliberately omitting WWW-Authenticate: sending it makes browsers
      // (including in-car WebViews) pop their own native login dialog on a
      // failed <audio> load, which the app has no control over and which
      // blocks the UI. The credentials are still required and validated
      // above — this only changes what the browser does on failure.
      console.log(`/stream 401 auth failed (has auth header: ${!!req.headers.authorization}, has user/pass params: ${reqUrl.searchParams.has('user')})`);
      res.writeHead(401, streamCors);
      return res.end();
    }
    const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress;
    if(isStreamRateLimited(ip)){
      console.log(`/stream 429 rate limited: ${ip}`);
      res.writeHead(429, streamCors);
      return res.end();
    }

    const streamUrl = reqUrl.searchParams.get('url');
    if(!streamUrl) return sendJson(res, 400, { error: 'Missing required "url" query parameter' });
    let parsedStream;
    try{ parsedStream = new URL(streamUrl); }catch{ return sendJson(res, 400, { error: 'Invalid stream URL' }); }
    if(parsedStream.protocol !== 'http:' && parsedStream.protocol !== 'https:'){
      return sendJson(res, 400, { error: 'Only http(s) stream URLs are supported' });
    }
    pipeStream(streamUrl, req, res, streamCors);
    return;
  }

  sendJson(res, 404, { error: 'Unknown route. Use /search?q= or /feed?url=' });
});

server.listen(PORT, () => {
  console.log(`Autopod proxy listening on port ${PORT}`);
});
