import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

async function startServer(t) {
  const fixture = await fs.mkdtemp(path.join(os.tmpdir(), 'joltrin-site-server-'));
  t.after(() => fs.rm(fixture, { recursive: true, force: true }));
  await fs.mkdir(path.join(fixture, 'scripts'));
  await fs.mkdir(path.join(fixture, '_site', 'docs'), { recursive: true });
  await fs.mkdir(path.join(fixture, '_site-private'));
  await fs.writeFile(path.join(fixture, '_site', 'index.html'), 'homepage');
  await fs.writeFile(path.join(fixture, '_site', 'docs', 'index.html'), 'docs');
  await fs.writeFile(path.join(fixture, '_site-private', 'secret.txt'), 'private fixture');
  const script = path.join(fixture, 'scripts', 'serve-test-site.mjs');
  await fs.copyFile(fileURLToPath(new URL('./serve-test-site.mjs', import.meta.url)), script);

  const reservation = net.createServer();
  reservation.listen(0, '127.0.0.1');
  await once(reservation, 'listening');
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const child = spawn(process.execPath, [script], {
    env: { ...process.env, PORT: String(port) }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill();
      await once(child, 'exit');
    }
  });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Server startup timed out: ${stderr}`)), 5000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Server exited ${code}: ${stderr}`)); });
    child.stdout.once('data', () => { clearTimeout(timer); resolve(); });
  });
  return async target => new Promise((resolve, reject) => {
    const request = http.get({ hostname: '127.0.0.1', port, path: target }, response => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', chunk => { body += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, body }));
    });
    request.on('error', reject);
    request.setTimeout(5000, () => request.destroy(new Error('Request timed out')));
  });
}

for (const target of ['/%', '/%E0%A4%A', '//[', '/%00']) {
  test(`rejects malformed target ${target} without stopping the server`, async t => {
    const request = await startServer(t);
    assert.equal((await request(target)).status, 400);
    assert.equal((await request('/')).body, 'homepage');
  });
}

test('rejects traversal into a sibling whose name shares the site prefix', async t => {
  const request = await startServer(t);
  const response = await request('/..%2f_site-private/secret.txt');
  assert.equal(response.status, 403);
  assert.ok(!response.body.includes('private fixture'));
  assert.equal((await request('/')).status, 200);
});

test('serves pages and redirects directories with their query intact', async t => {
  const request = await startServer(t);
  const redirect = await request('/docs?from=quickstart');
  assert.equal(redirect.status, 301);
  assert.equal(redirect.headers.location, '/docs/?from=quickstart');
  assert.equal((await request('/docs/')).body, 'docs');
  assert.equal((await request('/missing')).status, 404);
});
