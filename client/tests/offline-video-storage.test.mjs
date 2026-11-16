import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { build } from 'vite';


// BROWSER_PATH に Chrome / Chromium / Edge の実行ファイルを指定して yarn test:browser で実行する。
test('番組取得待ちに失敗ジョブを削除すると、実 IndexedDB への再登録を拒否する', {timeout: 30_000}, async (t) => {
    assert.ok(process.env.BROWSER_PATH, 'BROWSER_PATH に Chromium 系ブラウザーの実行ファイルを指定してください。');
    const [bundle] = await build({
        configFile: false,
        logLevel: 'silent',
        resolve: {alias: [
            {find: /^@\/services\/Videos$/, replacement: 'test-videos'},
            {find: /^@\/utils$/, replacement: 'test-utils'},
            {find: '@', replacement: fileURLToPath(new URL('../src', import.meta.url))},
        ]},
        plugins: [{
            name: 'offline-test-api',
            resolveId: id => ['test-videos', 'test-utils'].includes(id) ? `\0${id}` : null,
            load: id => id === '\0test-videos' ? 'export default {};' :
                id === '\0test-utils' ? 'export default {api_base_url: location.origin + "/api"};' : null,
        }],
        build: {
            write: false,
            target: 'esnext',
            minify: false,
            lib: {entry: fileURLToPath(new URL('./offline-video-storage.browser.js', import.meta.url)), formats: ['es']},
        },
    });
    const script = bundle.output.find(output => output.type === 'chunk').code;
    const result = Promise.withResolvers();
    const server = createServer((request, response) => {
        if (request.url === '/result' && request.method === 'POST') {
            let body = '';
            request.setEncoding('utf8');
            request.on('data', chunk => { body += chunk; });
            request.on('end', () => {
                response.end();
                result.resolve(body);
            });
            return;
        }
        response.setHeader('Content-Type', request.url === '/test.js' ? 'text/javascript' : 'text/html');
        response.end(request.url === '/test.js' ? script : '<!doctype html><body>RUNNING<script type="module" src="/test.js"></script></body>');
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    t.after(() => server.close());
    const profile = await mkdtemp(join(tmpdir(), 'konomitv-indexeddb-'));
    const browser = spawn(process.env.BROWSER_PATH, [
        '--headless', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
        `--user-data-dir=${profile}`, '--remote-debugging-pipe',
        `http://127.0.0.1:${server.address().port}/`,
    ], {windowsHide: true, stdio: ['ignore', 'ignore', 'pipe', 'pipe', 'pipe']});
    let stderr = '';
    browser.stderr.on('data', chunk => { stderr += chunk; });
    browser.on('error', result.reject);
    browser.on('exit', code => result.reject(new Error(`Browser exited (${code}): ${stderr}`)));
    t.after(async () => {
        if (browser.exitCode === null && browser.pid !== undefined) {
            const exited = once(browser, 'exit');
            // CDP の終了要求で子プロセスも閉じてから、一時プロファイルを回収する。
            browser.stdio[3].write(`${JSON.stringify({id: 1, method: 'Browser.close'})}\0`);
            const timeout = setTimeout(() => browser.kill(), 5000);
            await exited;
            clearTimeout(timeout);
        }
        await rm(profile, {recursive: true, force: true, maxRetries: 5, retryDelay: 200});
    });
    assert.equal(await result.promise, 'PASS');
});
