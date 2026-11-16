import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';

import ts from 'typescript';


// ブラウザの保存領域と通信だけを差し替え、実際の再接続・パーサー・完了処理を実行する。
const source = readFileSync(new URL('../src/services/OfflineVideos.ts', import.meta.url), 'utf8');
const {outputText} = ts.transpileModule(source, {
    compilerOptions: {target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS},
});
const streamMetadata = {
    video_id: 1, file_hash: 'recording', quality: '240p', duration_seconds: 12,
};
const sessionInfo = {
    ...streamMetadata,
    protocol_version: 2, generation_token: 'a'.repeat(32),
};
const segments = [Buffer.from('first segment'), Buffer.from('second segment')];

function uint32(...values) {
    const buffer = Buffer.alloc(values.length * 4);
    values.forEach((value, index) => buffer.writeUInt32BE(value, index * 4));
    return buffer;
}

function streamResponse(request, interrupted, info = sessionInfo) {
    const url = request === null ? null : new URL(request.url);
    const nextSequence = Number(url?.searchParams.get('next_sequence') ?? 0);
    const metadata = Buffer.from(JSON.stringify(url === null ? streamMetadata : {
        ...info, session_id: url.pathname.split('/').at(-1), next_sequence: nextSequence,
    }));
    const chunks = [Buffer.from('KTVODLP\n'), uint32(metadata.length), metadata];
    segments.forEach((segment, sequence) => {
        if (sequence >= nextSequence) {
            chunks.push(uint32(sequence, 6000, segment.length));
            chunks.push(interrupted && sequence === 1 ? segment.subarray(0, 3) : segment);
        }
    });
    if (!interrupted) chunks.push(uint32(0xffffffff, segments.length));
    return new Response(Buffer.concat(chunks));
}

function downloadFixture({interruptFirstResponse = false, failStorage = null, respond = null, beforeWrite = null, afterRead = null, shortenReconnectDelays = true, peerConnection = undefined} = {}) {
    const job = {
        job_id: 'job', video_id: 1, generation_id: 'new', quality: '240p',
        program: {
            id: 1, title: '保存する番組', channel: null,
            network_id: 32736, service_id: 1024, event_id: 12, start_time: '2026-11-10T12:00:00+09:00',
            recorded_video: {file_hash: 'recording', duration: 12},
        },
        state: 'Waiting', downloaded_bytes: 0, background_fetch_id: null, error: null,
    };
    const baseURL = 'https://konomitv.test/local/offline-videos/1';
    const previousVideo = {video_id: 1, generation_id: 'old'};
    let storedVideo = previousVideo;
    let savedJob = {...job};
    const cache = new Map([[`${baseURL}/old/playlist.m3u8`, Buffer.from('previous video')]]);
    const requests = [];
    const writes = [];
    const deletedGenerations = [];
    const storage = {
        eventTarget: new EventTarget(),
        getJobs: async () => [{...savedJob}],
        getJob: async jobID => jobID === savedJob.job_id ? {...savedJob} : null,
        putJobIfVideoIdle: async newJob => { savedJob = {...newJob}; },
        getGenerationBaseURL: (_videoID, generationID) => `${baseURL}/${generationID}`,
        getStoredVideo: async () => storedVideo,
        openCache: async () => ({
            put: async (url, response) => {
                if (failStorage === 'CacheStorage' && url.endsWith('/segments/1.ts')) {
                    throw new DOMException('CacheStorage is full', 'QuotaExceededError');
                }
                await beforeWrite?.(url);
                const body = Buffer.from(await response.arrayBuffer());
                await afterRead?.(url);
                cache.set(url, body);
                writes.push(url);
            },
            delete: async url => cache.delete(url),
        }),
        updateActiveJob: async (updatedJob) => {
            if (['Completed', 'Failed', 'Cancelled'].includes(savedJob.state)) return false;
            if (failStorage === 'IndexedDB' && cache.has(`${baseURL}/new/segments/1.ts`)) {
                throw new DOMException('IndexedDB write failed', 'UnknownError');
            }
            savedJob = {...updatedJob};
            return true;
        },
        completeJob: async (completedJob, video) => {
            if (['Completed', 'Failed', 'Cancelled'].includes(savedJob.state)) return false;
            storedVideo = video;
            savedJob = {...completedJob, state: 'Completed'};
            return true;
        },
        transitionActiveJobToTerminalState: async (_jobID, state, error) => {
            if (['Completed', 'Failed', 'Cancelled'].includes(savedJob.state)) return null;
            savedJob = {...savedJob, state, error};
            return savedJob;
        },
        deleteGeneration: async (_videoID, generationID) => {
            deletedGenerations.push(generationID);
            for (const url of cache.keys()) {
                if (url.startsWith(`${baseURL}/${generationID}/`)) cache.delete(url);
            }
        },
    };
    const fetch = async (input, init) => {
        const request = new Request(input, init);
        requests.push(request);
        const response = await respond?.(request);
        if (response !== undefined) return response;
        if (!request.url.includes('/offline-sessions/')) return new Response('asset');
        const info = {...sessionInfo, quality: savedJob.quality, file_hash: savedJob.program.recorded_video.file_hash};
        if (request.method === 'PUT') return Response.json(info, {status: 201});
        if (request.method === 'DELETE') return new Response(null, {status: 204});
        const isFirst = new URL(request.url).searchParams.get('connection_id') === '1';
        return streamResponse(request, interruptFirstResponse && isFirst, info);
    };
    const navigator = {};
    const videos = {
        getThumbnailURL: program => `https://konomitv.test/api/videos/${program.id}/thumbnail`,
        fetchVideo: async () => job.program,
    };
    const modules = {
        '@/services/OfflineVideoStorage': {default: storage},
        '@/services/Videos': {default: videos},
        '@/utils': {default: {api_base_url: 'https://konomitv.test/api'}},
    };
    const exports = {};
    const window = peerConnection === undefined ? {} : {RTCPeerConnection: peerConnection};
    new Function('require', 'exports', 'window', 'navigator', 'fetch', 'crypto', 'RTCPeerConnection', outputText)(
        name => modules[name], exports, window, navigator, fetch, webcrypto, peerConnection,
    );
    const service = exports.default;
    // 再接続の待ち時間だけを短縮する。試行・保存位置の判定は本体を使う。
    if (shortenReconnectDelays) service.RECONNECT_DELAYS = [0, 0, 0, 0, 0];
    return {
        run: (controller = new AbortController()) => service.runForegroundDownload(job, controller, null),
        service, storage, videos, navigator,
        cache, requests, writes, deletedGenerations, baseURL, previousVideo,
        get job() { return savedJob; },
        get video() { return storedVideo; },
    };
}

for (const failure of ['HTTP', 'HTTP エラー後の本文切断', '本文の途中切断', 'CacheStorage']) {
    test(`付随画像に失敗してもジョブを継続する (${failure})`, async () => {
        const bodyDisconnected = failure.includes('本文');
        const status = failure === 'CacheStorage' || failure === '本文の途中切断' ? 200 : 503;
        let bodyCancelled = false;
        const fixture = downloadFixture({respond: async () => new Response(new ReadableStream({
            start(controller) {
                if (bodyDisconnected) controller.error(new TypeError('Connection lost after receiving the image headers.'));
            },
            cancel() { bodyCancelled = true; },
        }), {status})});
        const controller = new AbortController();
        const cache = {put: async (_url, response) => {
            if (failure === 'CacheStorage') throw new DOMException('CacheStorage is full', 'QuotaExceededError');
            await response.arrayBuffer();
        }};
        await fixture.service.cacheOptionalAsset(cache, 'https://konomitv.test/image', '/image', controller.signal);
        assert.equal(bodyCancelled, !bodyDisconnected);
        assert.equal(controller.signal.aborted, false);
    });
}

test('付随画像の保存中に実装上の例外が発生したらジョブを失敗にする', async () => {
    const fixture = downloadFixture({beforeWrite(url) {
        if (url.endsWith('/thumbnail.webp')) throw new ReferenceError('Injected implementation error');
    }});
    fixture.job.state = 'Cancelled';
    await assert.rejects(fixture.service.start(fixture.job.program, '240p'), ReferenceError);
    assert.equal(fixture.job.state, 'Failed');
    assert.equal(fixture.video, fixture.previousVideo);
    assert.equal(fixture.requests.some(request => request.url.includes('/offline-sessions/')), false);
});

for (const useBackgroundFetch of [false, true]) {
    const mode = useBackgroundFetch ? 'Background Fetch' : '通常 Fetch';
    for (const waitingFor of ['ヘッダー', '本文']) {
        test(`${mode}: 画像の${waitingFor}待ちをキャンセルした後、同じ番組を保存し直せる`, {timeout: 5000}, async (t) => {
            const assetWaiting = Promise.withResolvers();
            let stalledRequest;
            let shouldStall = true;
            const fixture = downloadFixture({respond: async request => {
                if (!shouldStall || !new URL(request.url).pathname.endsWith('/thumbnail')) return;
                stalledRequest = request;
                if (waitingFor === 'ヘッダー') {
                    return await new Promise((_resolve, reject) => {
                        request.signal.addEventListener('abort', () => reject(request.signal.reason), {once: true});
                        assetWaiting.resolve();
                    });
                }
                return new Response(new ReadableStream({
                    start(controller) {
                        controller.enqueue(new TextEncoder().encode('partial image'));
                        request.signal.addEventListener('abort', () => controller.error(request.signal.reason), {once: true});
                    },
                    pull() { assetWaiting.resolve(); },
                }));
            }});
            fixture.job.state = 'Cancelled';
            const program = fixture.job.program;
            const releaseForegroundLock = t.mock.fn();
            t.mock.method(fixture.service, 'acquireForegroundLock', async () => releaseForegroundLock);
            const foregroundDownload = t.mock.method(fixture.service, 'runForegroundDownload');
            const backgroundFetch = t.mock.fn(async () => ({abort: async () => true}));
            fixture.navigator.serviceWorker = {getRegistration: async () => ({
                backgroundFetch: {fetch: backgroundFetch, get: async () => undefined},
            })};

            // 保存開始から実際の通信中断と後処理まで通し、旧世代が残ることも確認する。
            const starting = fixture.service.start(program, '240p', useBackgroundFetch);
            const cancelled = assert.rejects(starting, {name: 'AbortError'});
            await assetWaiting.promise;
            const cancelledJobID = fixture.job.job_id;
            await fixture.service.cancel(cancelledJobID);
            await cancelled;
            assert.equal(stalledRequest.signal.aborted, true);
            assert.equal(fixture.job.state, 'Cancelled');
            assert.equal(releaseForegroundLock.mock.callCount(), useBackgroundFetch ? 0 : 1);
            assert.equal(backgroundFetch.mock.callCount(), 0);
            assert.equal(fixture.video, fixture.previousVideo);
            assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);

            // 開始用のロックも解放され、同じ番組の新しいジョブを登録できる。
            shouldStall = false;
            const restarted = await fixture.service.start(program, '240p', useBackgroundFetch);
            assert.notEqual(restarted.job_id, cancelledJobID);
            if (useBackgroundFetch) {
                assert.equal(backgroundFetch.mock.callCount(), 1);
                await fixture.service.cancel(restarted.job_id);
            } else {
                await foregroundDownload.mock.calls[0].result;
                assert.equal(fixture.job.state, 'Completed');
                assert.equal(releaseForegroundLock.mock.callCount(), 2);
            }

            // 付随データはブラウザが既存の HTTP キャッシュを利用できる要求にする。
            const assetRequests = fixture.requests.filter(request => !request.url.includes('/offline-sessions/'));
            assert.ok(assetRequests.length > 0);
            assert.ok(assetRequests.every(request => request.cache === 'default'));
        });
    }

    test(`${mode}: キャンセル後に完了する画像の書き込みも回収する`, {timeout: 5000}, async (t) => {
        const writeStarted = Promise.withResolvers();
        const finishWrite = Promise.withResolvers();
        const fixture = downloadFixture({afterRead: async (url) => {
            if (url.endsWith('/thumbnail.webp')) {
                writeStarted.resolve();
                await finishWrite.promise;
            }
        }});
        fixture.job.state = 'Cancelled';
        const releaseForegroundLock = t.mock.fn();
        t.mock.method(fixture.service, 'acquireForegroundLock', async () => releaseForegroundLock);
        const foregroundDownload = t.mock.method(fixture.service, 'runForegroundDownload');
        const backgroundFetch = t.mock.fn();
        fixture.navigator.serviceWorker = {getRegistration: async () => ({backgroundFetch: {fetch: backgroundFetch, get: async () => undefined}})};

        const starting = fixture.service.start(fixture.job.program, '240p', useBackgroundFetch);
        const cancelled = assert.rejects(starting, {name: 'AbortError'});
        await writeStarted.promise;
        const cancelling = fixture.service.cancel(fixture.job.job_id);
        // 本文を受信済みの cache.put() は、Fetch の abort だけでは終了しない。
        await setImmediate();
        finishWrite.resolve();
        await cancelling;
        await cancelled;
        assert.equal(foregroundDownload.mock.callCount(), 0);
        assert.equal(backgroundFetch.mock.callCount(), 0);
        assert.equal(releaseForegroundLock.mock.callCount(), useBackgroundFetch ? 0 : 1);
        assert.ok(fixture.writes.some(url => url.endsWith('/thumbnail.webp')));
        assert.equal(fixture.video, fixture.previousVideo);
        assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
    });

    test(`${mode}: 手動再試行は画質・保存方式を引き継ぎ、最新の番組情報で新しく開始する`, async (t) => {
        const fixture = downloadFixture();
        fixture.job.state = 'Failed';
        fixture.job.quality = '720p-hevc-10bit-24fps';
        fixture.job.background_fetch_id = useBackgroundFetch ? 'previous-background-fetch' : null;
        const failedJob = {...fixture.job};
        const latestProgram = {
            ...failedJob.program, title: '更新後の番組情報',
            recorded_video: {...failedJob.program.recorded_video, file_hash: 'updated-recording'},
        };
        const fetchVideo = t.mock.method(fixture.videos, 'fetchVideo', async () => latestProgram);
        const registerJob = t.mock.method(fixture.storage, 'putJobIfVideoIdle');
        const foregroundDownload = t.mock.method(fixture.service, 'runForegroundDownload');
        const backgroundFetch = t.mock.fn(async () => ({abort: async () => true}));
        fixture.navigator.serviceWorker = {getRegistration: async () => ({backgroundFetch: {fetch: backgroundFetch}})};

        const restarted = await fixture.service.retry(failedJob.job_id);
        assert.deepEqual(fetchVideo.mock.calls[0].arguments, [failedJob.video_id]);
        assert.deepEqual(registerJob.mock.calls[0].arguments, [restarted, failedJob.job_id]);
        assert.equal(restarted.quality, failedJob.quality);
        assert.deepEqual(restarted.program, latestProgram);
        assert.notEqual(restarted.job_id, failedJob.job_id);
        assert.notEqual(restarted.generation_id, failedJob.generation_id);
        if (useBackgroundFetch) {
            assert.equal(foregroundDownload.mock.callCount(), 0);
            assert.notEqual(restarted.background_fetch_id, failedJob.background_fetch_id);
            const [id, requests] = backgroundFetch.mock.calls[0].arguments;
            assert.equal(id, restarted.background_fetch_id);
            assert.equal(new URL(requests[0].url).pathname, `/api/streams/video/1/${failedJob.quality}/offline-stream`);
        } else {
            assert.equal(backgroundFetch.mock.callCount(), 0);
            assert.equal(restarted.background_fetch_id, null);
            await foregroundDownload.mock.calls[0].result;
            assert.equal(fixture.job.state, 'Completed');
        }
    });
}

test('録画 ID が再利用されても、別の放送回を再試行で登録せず保存済み動画を保持する', async (t) => {
    const fixture = downloadFixture();
    fixture.job.state = 'Failed';
    const failedJob = fixture.job;
    t.mock.method(fixture.videos, 'fetchVideo', async () => ({
        ...failedJob.program,
        // 番組番号が循環した後の別の放送回を、DB が同じ録画 ID で返す。
        start_time: '2026-11-12T12:00:00+09:00',
        recorded_video: {...failedJob.program.recorded_video, file_hash: 'different-recording'},
    }));
    const registerJob = t.mock.method(fixture.storage, 'putJobIfVideoIdle');

    await assert.rejects(fixture.service.retry(failedJob.job_id), /同じ録画番組か確認できませんでした/);
    assert.equal(registerJob.mock.callCount(), 0);
    assert.equal(fixture.job.job_id, failedJob.job_id);
    assert.equal(fixture.job.state, 'Failed');
    assert.equal(fixture.video, fixture.previousVideo);
    assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
});

for (const useBackgroundFetch of [false, true]) {
    const mode = useBackgroundFetch ? 'Background Fetch' : '通常 Fetch';
    test(`${mode}: 画像の受信が30秒を超えても最後まで保存する`, {timeout: 5000}, async (t) => {
        t.mock.timers.enable({apis: ['setTimeout']});
        const bodyStarted = Promise.withResolvers();
        let imageBody;
        const fixture = downloadFixture({respond: (request) => {
            if (!new URL(request.url).pathname.endsWith('/thumbnail/tiled')) return;
            return new Response(new ReadableStream({
                start(controller) {
                    imageBody = controller;
                    controller.enqueue(Buffer.from('first '));
                    request.signal.addEventListener('abort', () => controller.error(request.signal.reason), {once: true});
                },
                pull() { bodyStarted.resolve(request); },
            }));
        }});
        fixture.job.state = 'Cancelled';
        const foregroundDownload = t.mock.method(fixture.service, 'runForegroundDownload');
        const backgroundFetch = t.mock.fn(async () => ({abort: async () => true}));
        fixture.navigator.serviceWorker = {getRegistration: async () => ({backgroundFetch: {fetch: backgroundFetch}})};

        const starting = fixture.service.start(fixture.job.program, '240p', useBackgroundFetch);
        const request = await bodyStarted.promise;
        t.mock.timers.tick(20_000);
        imageBody.enqueue(Buffer.from('second '));
        await setImmediate();
        t.mock.timers.tick(20_000);
        await setImmediate();
        assert.equal(request.signal.aborted, false);
        assert.equal(foregroundDownload.mock.callCount(), 0);
        assert.equal(backgroundFetch.mock.callCount(), 0);

        imageBody.enqueue(Buffer.from('last'));
        imageBody.close();
        await starting;
        if (useBackgroundFetch) {
            assert.equal(backgroundFetch.mock.callCount(), 1);
        } else {
            await foregroundDownload.mock.calls[0].result;
            assert.equal(fixture.job.state, 'Completed');
        }
        const imageURL = fixture.writes.find(url => url.endsWith('/thumbnail-tiled.webp'));
        assert.equal(fixture.cache.get(imageURL)?.toString(), 'first second last');
    });
}

test('Background Fetch の展開では進捗を間引き、各セグメントでは更新しない', async (t) => {
    t.mock.method(performance, 'now', () => 0);
    const fixture = downloadFixture();
    fixture.job.background_fetch_id = 'background';
    const update = t.mock.method(fixture.storage, 'updateActiveJob');
    await fixture.service.finalizeResponse(fixture.job.job_id, streamResponse(null, false));
    assert.equal(update.mock.callCount(), 2);  // Downloading と Finalizing の開始時だけ
    assert.equal(fixture.job.state, 'Completed');
});

test('サムネイルの書き込みに失敗しても Background Fetch を開始する', async (t) => {
    const fixture = downloadFixture({beforeWrite: url => {
        if (url.endsWith('/thumbnail.webp')) throw new DOMException('Image could not be saved', 'QuotaExceededError');
    }});
    fixture.job.state = 'Cancelled';
    const backgroundFetch = t.mock.fn(async () => ({abort: async () => true}));
    fixture.navigator.serviceWorker = {getRegistration: async () => ({backgroundFetch: {fetch: backgroundFetch}})};

    await fixture.service.start(fixture.job.program, '240p', true);
    assert.equal(backgroundFetch.mock.callCount(), 1);
    assert.equal(fixture.job.state, 'Waiting');
    assert.ok(fixture.writes.every(url => !url.endsWith('/thumbnail.webp')));
});

for (const [phase, scenario] of [
    ['Cancel', 'WebRTC の Offer が未解決でもキャンセルでロックを解放する'],
    ['Offer', 'WebRTC の Offer が未解決でも5秒後に動画保存を開始する'],
    ['ICE', 'ICE 候補の収集が止まっても5秒後に動画保存を開始する'],
    ['DataChannel', 'WebRTC の準備を段階ごとに5秒待つ'],
]) {
    const shouldCancel = phase === 'Cancel';
    test(scenario, {timeout: 5000}, async (t) => {
        t.mock.timers.enable({apis: ['setTimeout']});
        const offerStarted = Promise.withResolvers();
        const closePeer = t.mock.fn();
        const peers = [];
        const dataChannel = Object.assign(new EventTarget(), {readyState: 'connecting', close() {}});
        class PeerConnection extends EventTarget {
            iceGatheringState = 'gathering';
            localDescription = null;
            constructor() {
                super();
                peers.push(this);
            }
            createDataChannel() { return dataChannel; }
            close() { closePeer(); }
            createOffer() {
                offerStarted.resolve();
                // ブラウザは接続を閉じると作成中の Offer を確定しない場合がある。
                return shouldCancel || phase === 'Offer' ? new Promise(() => {}) : Promise.resolve({});
            }
            async createAnswer() { return {}; }
            async setLocalDescription(description) {
                this.localDescription = description;
            }
            async setRemoteDescription() {}
        }

        const fixture = downloadFixture({peerConnection: PeerConnection});
        fixture.job.state = 'Cancelled';
        const releaseForegroundLock = t.mock.fn();
        t.mock.method(fixture.service, 'acquireForegroundLock', async () => releaseForegroundLock);
        const foregroundDownload = t.mock.method(fixture.service, 'runForegroundDownload');

        const job = await fixture.service.start(fixture.job.program, '240p');
        await offerStarted.promise;
        if (shouldCancel) {
            await fixture.service.cancel(job.job_id);
        } else {
            await setImmediate();
            if (phase === 'DataChannel') {
                // Offer と Answer がそれぞれ期限直前に準備できても、DataChannel に次の5秒を使う。
                for (const peer of peers) {
                    t.mock.timers.tick(4999);
                    assert.equal(closePeer.mock.callCount(), 0);
                    peer.iceGatheringState = 'complete';
                    peer.dispatchEvent(new Event('icegatheringstatechange'));
                    await setImmediate();
                }
            }
            t.mock.timers.tick(4999);
            assert.equal(closePeer.mock.callCount(), 0);
            assert.equal(fixture.requests.some(request => request.url.includes('/offline-sessions/')), false);
            t.mock.timers.tick(1);
        }
        await foregroundDownload.mock.calls[0].result;

        assert.equal(fixture.job.state, shouldCancel ? 'Cancelled' : 'Completed');
        assert.equal(releaseForegroundLock.mock.callCount(), 1);
        assert.equal(closePeer.mock.callCount(), 2);
        assert.equal(fixture.requests.some(request => request.url.includes('/offline-sessions/')), !shouldCancel);
    });
}

test('Background Fetch は断片の回収にも失敗した場合に元の保存エラーを記録する', async (t) => {
    const fixture = downloadFixture({failStorage: 'CacheStorage'});
    fixture.job.background_fetch_id = 'background';
    const cleanup = t.mock.method(fixture.storage, 'deleteGeneration', async () => {
        throw new Error('Injected cleanup failure');
    });
    await assert.rejects(async () => {
        const jobID = fixture.job.job_id;
        try {
            await fixture.service.finalizeResponse(jobID, streamResponse(null, false));
        } catch (error) {
            await fixture.service.markJobFailed(jobID, error.message);
            throw error;
        }
    }, {name: 'QuotaExceededError'});
    assert.equal(fixture.job.state, 'Failed');
    assert.equal(fixture.job.error, 'CacheStorage is full');
    assert.equal(cleanup.mock.callCount(), 1);
    assert.equal(fixture.video, fixture.previousVideo);
});

test('実況の受信が30秒を超えても、動画の保存完了後まで取得を続ける', async (t) => {
    t.mock.timers.enable({apis: ['setTimeout']});
    let commentsRequest;
    let commentsBody;
    const fixture = downloadFixture({respond: async request => {
        if (!new URL(request.url).pathname.endsWith('/jikkyo')) return;
        commentsRequest = request;
        return new Response(new ReadableStream({start(controller) {
            commentsBody = controller;
            request.signal.addEventListener('abort', () => controller.error(request.signal.reason), {once: true});
        }}));
    }});
    const controller = new AbortController();
    const destination = `${fixture.baseURL}/new/assets/jikkyo.json`;
    try {
        await fixture.service.prepareAssets(fixture.job, controller.signal);
        await fixture.run(controller);
        assert.equal(fixture.job.state, 'Completed');
        assert.equal(fixture.cache.has(destination), false);

        // 受信は5秒ごとに進むが、全体の取得時間は画像の30秒期限を超える。
        const chunks = ['{"comments":[', '"1",', '"2",', '"3",', '"4",', '"5",', '"6"]}'];
        for (const chunk of chunks) {
            commentsBody.enqueue(new TextEncoder().encode(chunk));
            t.mock.timers.tick(5000);
            await setImmediate();
            assert.equal(commentsRequest.signal.aborted, false);
        }
        commentsBody.close();
        await setImmediate();
        assert.equal(fixture.cache.get(destination).toString(), chunks.join(''));
        assert.equal(fixture.job.state, 'Completed');
    } finally {
        controller.abort();
        await setImmediate();
    }
});

test('途中切断後は保存済みセグメントの次から再接続して完成する', async () => {
    const fixture = downloadFixture({interruptFirstResponse: true});
    await fixture.run();
    const connections = fixture.requests.filter(request => request.method === 'GET').map(request => new URL(request.url));
    assert.deepEqual(connections.map(url => url.searchParams.get('next_sequence')), ['0', '1']);
    assert.deepEqual(connections.map(url => url.searchParams.get('connection_id')), ['1', '2']);
    assert.equal(connections[0].pathname, connections[1].pathname);
    assert.equal(fixture.requests.filter(request => request.method === 'PUT').length, 1);
    segments.forEach((segment, sequence) => {
        const url = `${fixture.baseURL}/new/segments/${sequence}.ts`;
        assert.deepEqual(fixture.cache.get(url), segment);
        assert.equal(fixture.writes.filter(write => write === url).length, 1);
    });
    assert.equal(fixture.job.state, 'Completed');
    assert.equal(fixture.video.segment_count, 2);
    assert.equal(fixture.video.size_bytes, segments[0].length + segments[1].length);
    assert.match(fixture.cache.get(`${fixture.baseURL}/new/playlist.m3u8`).toString(), /#EXT-X-ENDLIST/);
    assert.equal(fixture.requests.at(-1).method, 'DELETE');
});

test('通常 Fetch は32MiBを超えるセグメントを保存せず、以前の保存済み動画を保持する', async () => {
    const fixture = downloadFixture({respond: async (request) => {
        if (request.method !== 'GET') return;
        const body = Buffer.from(await streamResponse(request, false).arrayBuffer());
        const metadataEnd = 12 + body.readUInt32BE(8);
        body.writeUInt32BE(32 * 1024 * 1024 + 1, metadataEnd + 8);
        return new Response(body);
    }});
    await fixture.run();
    assert.equal(fixture.job.state, 'Failed');
    assert.match(fixture.job.error, /セグメントの長さが不正/);
    assert.equal(fixture.requests.filter(request => request.method === 'GET').length, 1);
    assert.equal(fixture.writes.length, 0);
    assert.equal(fixture.video, fixture.previousVideo);
    assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
});

test('初回作成の応答喪失と終端喪失から同じ保存を再開し、保存済みデータを重複させない', async () => {
    let createCount = 0;
    let connectionCount = 0;
    const fixture = downloadFixture({respond: async (request) => {
        if (request.method === 'PUT') {
            if (++createCount === 1) throw new TypeError('Injected lost creation response');
            return Response.json(sessionInfo);
        }
        if (request.method === 'DELETE') return new Response(null, {status: 204});
        const response = streamResponse(request, false);
        if (++connectionCount === 1) {
            const body = Buffer.from(await response.arrayBuffer());
            return new Response(body.subarray(0, body.length - 8));
        }
        return response;
    }});
    await fixture.run();
    assert.equal(createCount, 2);
    assert.equal(new Set(fixture.requests.map(request => new URL(request.url).pathname)).size, 1);
    const connections = fixture.requests.filter(request => request.method === 'GET');
    assert.deepEqual(connections.map(request => new URL(request.url).searchParams.get('next_sequence')), ['0', '2']);
    assert.equal(fixture.writes.filter(url => url.includes('/segments/')).length, 2);
    assert.equal(fixture.job.state, 'Completed');
});

for (const status of [410, 500]) {
    test(`再接続の HTTP ${status} では再作成せず、以前の保存済み動画を保持する`, async () => {
        let connectionCount = 0;
        const fixture = downloadFixture({respond: async (request) => {
            if (request.method === 'PUT') return Response.json(sessionInfo, {status: 201});
            if (request.method === 'DELETE') return new Response(null, {status: 204});
            if (++connectionCount === 1) return streamResponse(request, true);
            return new Response(null, {status});
        }});
        await fixture.run();
        assert.equal(connectionCount, 2);
        assert.equal(fixture.requests.filter(request => request.method === 'PUT').length, 1);
        assert.equal(fixture.job.state, 'Failed');
        assert.equal(fixture.video, fixture.previousVideo);
        assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
    });
}

test('キャンセル後に完了するセグメントの書き込みも回収し、以前の保存済み動画を保持する', async () => {
    const writeStarted = Promise.withResolvers();
    const finishWrite = Promise.withResolvers();
    const fixture = downloadFixture({beforeWrite: async (url) => {
        if (url.endsWith('/segments/1.ts')) {
            writeStarted.resolve();
            await finishWrite.promise;
        }
    }});
    const controller = new AbortController();
    const download = fixture.run(controller);
    await writeStarted.promise;
    controller.abort();
    finishWrite.resolve();
    await download;
    assert.equal(fixture.job.state, 'Cancelled');
    assert.equal(fixture.requests.filter(request => request.method === 'GET').length, 1);
    assert.equal(fixture.video, fixture.previousVideo);
    assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
});

for (const {limit, responseDelay, expectedStarts} of [
    {limit: '5回', responseDelay: 0, expectedStarts: [0, 1000, 3000, 7000, 15000, 30000]},
    {limit: '45秒', responseDelay: 15000, expectedStarts: [0, 16000, 33000, 52000]},
]) {
    test(`保存が進まなければ再接続を${limit}で打ち切り、メタデータ・keepalive・部分受信で予算を戻さない`, async (t) => {
        let now = 0;
        t.mock.timers.enable({apis: ['setTimeout']});
        t.mock.method(performance, 'now', () => now);
        t.mock.method(Math, 'random', () => 0.5);
        const connectionStarts = [];
        const fixture = downloadFixture({shortenReconnectDelays: false, respond: async (request) => {
            if (request.method === 'PUT') return Response.json(sessionInfo, {status: 201});
            if (request.method === 'DELETE') return new Response(null, {status: 204});
            connectionStarts.push(now);

            // 応答の待ち時間も予算に含め、200 の後はセグメントを保存し切る前に切断する。
            now += responseDelay;
            t.mock.timers.tick(responseDelay);
            const body = Buffer.from(await streamResponse(request, false).arrayBuffer());
            const metadataEnd = 12 + body.readUInt32BE(8);
            return new Response(Buffer.concat([
                body.subarray(0, metadataEnd),
                uint32(0xfffffffe, 0),
                body.subarray(metadataEnd, metadataEnd + 12 + 3),
            ]));
        }});
        const controller = new AbortController();
        const download = fixture.run(controller);
        try {
            await setImmediate();
            // 上限の判定が壊れた場合も、テスト自体は有限回で終了する。
            for (let tick = 0; tick < 120 && fixture.job.state !== 'Failed'; tick++) {
                now += 1000;
                t.mock.timers.tick(1000);
                await setImmediate();
            }
            assert.equal(fixture.job.state, 'Failed');
            assert.match(fixture.job.error, /通信を再開できませんでした/);
            assert.deepEqual(connectionStarts, expectedStarts);
            const connections = fixture.requests.filter(request => request.method === 'GET');
            assert.ok(connections.every(request => new URL(request.url).searchParams.get('next_sequence') === '0'));
            assert.equal(fixture.requests.filter(request => request.method === 'PUT').length, 1);
            assert.equal(fixture.writes.length, 0);
            assert.equal(fixture.video, fixture.previousVideo);
        } finally {
            controller.abort();
            await download;
        }
    });
}

test('保存完了通知の失敗後に再送し、長いセグメントの受信中も生存通知を続ける', async (t) => {
    // 本体のタイマーと経過時間を進め、90秒以上かかる受信を実時間の待機なしで確認する。
    let now = 0;
    t.mock.timers.enable({apis: ['setTimeout']});
    t.mock.method(performance, 'now', () => now);
    const checkpoints = [];
    let body;
    let bodyOffset;
    let bodyController;
    const fixture = downloadFixture({respond: async (request) => {
        if (new URL(request.url).pathname.endsWith('/checkpoint')) {
            const checkpoint = await request.json();
            checkpoints.push({at: now, ...checkpoint});
            assert.equal(request.headers.get('X-KonomiTV-Offline-Generation'), sessionInfo.generation_token);
            if (checkpoints.length === 1) throw new TypeError('Injected checkpoint disconnect');
            return Response.json(checkpoint);
        }
        if (request.method === 'PUT') return Response.json(sessionInfo, {status: 201});
        if (request.method === 'DELETE') return new Response(null, {status: 204});

        body = Buffer.from(await streamResponse(request, false).arrayBuffer());
        // 1件目を保存させ、2件目は本文を少しずつ受信する。
        bodyOffset = 12 + body.readUInt32BE(8) + 12 + segments[0].length + 12;
        return new Response(new ReadableStream({start(controller) {
            bodyController = controller;
            controller.enqueue(body.subarray(0, bodyOffset));
        }}));
    }});
    const download = fixture.run();
    await setImmediate();
    assert.equal(fixture.writes.length, 1);

    for (let second = 1; second <= 96; second++) {
        now += 1000;
        if (second % 20 === 0) {
            bodyController.enqueue(body.subarray(bodyOffset, bodyOffset + 1));
            bodyOffset++;
        }
        t.mock.timers.tick(1000);
        await setImmediate();
    }
    assert.deepEqual(checkpoints, [3000, 6000, 16000, 26000, 36000, 46000, 56000, 66000, 76000, 86000, 96000].map(at => ({
        at, connection_id: 1, next_sequence: 1,
    })));
    assert.equal(fixture.job.state, 'Downloading');

    bodyController.enqueue(body.subarray(bodyOffset));
    bodyController.close();
    await download;
    assert.equal(fixture.job.state, 'Completed');
    assert.equal(fixture.requests.filter(request => request.method === 'GET').length, 1);
    assert.equal(fixture.requests.at(-1).method, 'DELETE');
});

for (const failStorage of ['CacheStorage', 'IndexedDB']) {
    test(`${failStorage} の失敗では再接続せず、以前の保存済み動画を保持する`, async () => {
        const fixture = downloadFixture({failStorage});
        await fixture.run();
        assert.equal(fixture.requests.filter(request => request.method === 'GET').length, 1);
        assert.equal(fixture.job.state, 'Failed');
        assert.match(fixture.job.error, new RegExp(failStorage));
        assert.equal(fixture.video, fixture.previousVideo);
        assert.deepEqual([...fixture.cache.keys()], [`${fixture.baseURL}/old/playlist.m3u8`]);
        assert.deepEqual(fixture.deletedGenerations, ['new']);
        assert.equal(fixture.requests.at(-1).method, 'DELETE');
    });
}
