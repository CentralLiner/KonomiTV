import OfflineVideos from '../src/services/OfflineVideos';
import OfflineVideoStorage from '../src/services/OfflineVideoStorage';

import Videos from '@/services/Videos';


function assert(condition, message) {
    if (!condition) throw new Error(message);
}

// 専用プロファイルの実 IndexedDB / CacheStorage と Web Locks を使う。
async function run() {
    const program = {id: 1, title: '再試行する番組', channel: null, recorded_video: {duration: 12, file_hash: 'recording'}};
    const oldJob = {
        job_id: 'completed', video_id: 1, generation_id: 'old', program, quality: '240p',
        state: 'Waiting', estimated_size_bytes: 100, downloaded_bytes: 0, background_fetch_id: null, error: null,
    };
    const savedVideo = {
        video_id: 1, generation_id: 'old', program, quality: '240p', file_hash: 'recording',
        size_bytes: 100, segment_count: 1, saved_at: 1,
    };
    await OfflineVideoStorage.putJobIfVideoIdle(oldJob);
    const cache = await OfflineVideoStorage.openCache();
    const playlistURL = `${OfflineVideoStorage.getGenerationBaseURL(1, 'old')}/playlist.m3u8`;
    await cache.put(playlistURL, new Response('saved playlist'));
    assert(await OfflineVideoStorage.completeJob(oldJob, savedVideo), '保存済み動画を準備できない');
    const failedJob = {...oldJob, job_id: 'failed', generation_id: 'failed'};
    await OfflineVideoStorage.putJobIfVideoIdle(failedJob);
    await OfflineVideoStorage.transitionActiveJobToTerminalState(failedJob.job_id, 'Failed', '通信切断');

    const fetching = Promise.withResolvers();
    const fetched = Promise.withResolvers();
    Videos.fetchVideo = async () => {
        fetching.resolve();
        return await fetched.promise;
    };
    // 誤って登録まで進んだ場合も本物の通信や変換は開始しない。
    window.fetch = async () => { throw new Error('再試行の拒否前に通信が始まった'); };
    const retry = OfflineVideos.retry(failedJob.job_id).then(() => null, error => error);
    await fetching.promise;
    await OfflineVideos.dismissJob(failedJob.job_id);
    assert(await OfflineVideoStorage.getJob(failedJob.job_id) === null, '失敗ジョブの削除が未完了');
    fetched.resolve(program);

    const error = await retry;
    assert(error?.message === 'やり直す保存が見つかりませんでした。', `再試行を登録時に拒否していない: ${error}`);
    const jobs = await OfflineVideoStorage.getJobs();
    assert(jobs.length === 1 && jobs[0].job_id === oldJob.job_id && jobs[0].state === 'Completed', '新ジョブが残っている');
    assert(JSON.stringify(await OfflineVideoStorage.getStoredVideo(1)) === JSON.stringify(savedVideo), '保存済み動画が変わった');
    assert(await (await cache.match(playlistURL))?.text() === 'saved playlist', '保存済みのプレイリストが消えた');
}

const report = window.fetch.bind(window);
try {
    await run();
    document.body.textContent = 'PASS';
} catch (error) {
    document.body.textContent = `FAIL: ${error.stack}`;
}
await report('/result', {method: 'POST', body: document.body.textContent});
