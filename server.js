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
 *
 * No external dependencies — pure Node.js (`http`/`https` only).
 */

const http = require('http');
const https = require('https');

const PORT = process.env.PORT || 3000;
const REQUEST_TIMEOUT_MS = 10000;

function corsHeaders(extra={}){
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
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

  sendJson(res, 404, { error: 'Unknown route. Use /search?q= or /feed?url=' });
});

server.listen(PORT, () => {
  console.log(`Autopod proxy listening on port ${PORT}`);
});
