import OfflineVideoStorage from '@/services/OfflineVideoStorage';
import Videos, { type IRecordedProgram } from '@/services/Videos';
import Utils from '@/utils';


/** オフライン保存ジョブの状態 */
export type OfflineDownloadState = 'Waiting' | 'Downloading' | 'Finalizing' | 'Completed' | 'Failed' | 'Cancelled';

/** オフライン保存済み動画 */
export interface IOfflineVideo {
    video_id: number;
    generation_id: string;
    file_hash: string;
    program: IRecordedProgram;
    quality: string;
    size_bytes: number;
    segment_count: number;
    saved_at: number;
}

/** オフライン保存ジョブ */
export interface IOfflineDownloadJob {
    job_id: string;
    video_id: number;
    generation_id: string;
    program: IRecordedProgram;
    quality: string;
    state: OfflineDownloadState;
    estimated_size_bytes: number;
    downloaded_bytes: number;
    background_fetch_id: string | null;
    error: string | null;
}

interface IOfflineVideoStreamMetadata {
    video_id: number;
    file_hash: string;
    quality: string;
    duration_seconds: number;
}

interface IOfflineVideoSessionInfo extends IOfflineVideoStreamMetadata {
    protocol_version: 2;
    generation_token: string;
}

interface IOfflineVideoConnection {
    sessionID: string;
    info: IOfflineVideoSessionInfo;
    controller: AbortController;
}

interface IOfflineDownloadProgress {
    segmentDurations: number[];
    sizeBytes: number;
}

/** fetch() やストリームの読み取りで発生し、同じ保存を再接続できるエラー */
class OfflineDownloadNetworkError extends Error {
    constructor(message: string, readonly retryAfterMilliseconds = 0, readonly status?: number) {
        super(message);
    }
}

/**
 * 録画番組のオフライン保存ジョブとダウンロード応答の展開を管理する。
 */
export default class OfflineVideos {

    static readonly eventTarget = OfflineVideoStorage.eventTarget;

    /** 保存画質セレクタと同じ利用者向けラベル */
    private static readonly QUALITY_LABELS: Record<string, string> = {
        '1080p-60fps': '1080p (60fps)',
        '1080p': '1080p',
        '810p': '810p',
        '720p': '720p',
        '540p': '540p',
        '480p': '480p',
        '360p': '360p',
        '240p': '240p',
    };

    /** 保存画質セレクタの基底値 (HEVC / 24fps 等の接尾辞は含まない) */
    static readonly BASE_QUALITY_VALUES = [
        '1080p-60fps',
        '1080p',
        '810p',
        '720p',
        '540p',
        '480p',
        '360p',
        '240p',
    ] as const;

    /** server/app/constants.py QUALITY と同じエンコード設定 (表示用の平均ビットレートと上限ビットレート) */
    private static readonly STREAM_QUALITY_BITRATES: Record<string, {video_bitrate_kbps: number; video_bitrate_max_kbps: number; audio_bitrate_kbps: number}> = {
        '1080p-60fps': {video_bitrate_kbps: 9500, video_bitrate_max_kbps: 13000, audio_bitrate_kbps: 256},
        '1080p-60fps-hevc': {video_bitrate_kbps: 3500, video_bitrate_max_kbps: 5200, audio_bitrate_kbps: 192},
        '1080p': {video_bitrate_kbps: 9500, video_bitrate_max_kbps: 13000, audio_bitrate_kbps: 256},
        '1080p-hevc': {video_bitrate_kbps: 3000, video_bitrate_max_kbps: 4500, audio_bitrate_kbps: 192},
        '810p': {video_bitrate_kbps: 5500, video_bitrate_max_kbps: 7600, audio_bitrate_kbps: 192},
        '810p-hevc': {video_bitrate_kbps: 2500, video_bitrate_max_kbps: 3700, audio_bitrate_kbps: 192},
        '720p': {video_bitrate_kbps: 4500, video_bitrate_max_kbps: 6200, audio_bitrate_kbps: 192},
        '720p-hevc': {video_bitrate_kbps: 2000, video_bitrate_max_kbps: 3000, audio_bitrate_kbps: 192},
        '540p': {video_bitrate_kbps: 3000, video_bitrate_max_kbps: 4100, audio_bitrate_kbps: 192},
        '540p-hevc': {video_bitrate_kbps: 1400, video_bitrate_max_kbps: 2100, audio_bitrate_kbps: 192},
        '480p': {video_bitrate_kbps: 2000, video_bitrate_max_kbps: 2800, audio_bitrate_kbps: 192},
        '480p-hevc': {video_bitrate_kbps: 1050, video_bitrate_max_kbps: 1750, audio_bitrate_kbps: 192},
        '360p': {video_bitrate_kbps: 1100, video_bitrate_max_kbps: 1800, audio_bitrate_kbps: 128},
        '360p-hevc': {video_bitrate_kbps: 750, video_bitrate_max_kbps: 1250, audio_bitrate_kbps: 128},
        '240p': {video_bitrate_kbps: 550, video_bitrate_max_kbps: 650, audio_bitrate_kbps: 128},
        '240p-hevc': {video_bitrate_kbps: 450, video_bitrate_max_kbps: 650, audio_bitrate_kbps: 128},
    };

    /** 空き容量判定向けの上限ビットレート余裕係数 */
    private static readonly REQUIRED_STORAGE_SIZE_MARGIN = 1.1;

    /**
     * VBR / QVBR エンコード (-vb, --vbr, --qvbr) では constants.py の video_bitrate は目標値であり、
     * 実際の映像平均は番組内容次第で目標より低く収まる
     * 720p-hevc・30分で 470MB 見積もり→370MB 実測のような乖離を抑える係数
     */
    private static readonly VBR_EFFECTIVE_VIDEO_BITRATE_RATIO = {
        hevc: 0.75,
        avc: 0.85,
    } as const;

    /** 進捗バー見積もり向けの余裕係数 (実効平均を基準にするため、表示見積もりより小さく保つ) */
    private static readonly JOB_PROGRESS_SIZE_MARGIN = 1.05;

    private static readonly STREAM_MAGIC = new TextEncoder().encode('KTVODLP\n');
    private static readonly MAX_SEGMENT_BYTES = 32 * 1024 * 1024;
    private static readonly RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 15000];
    private static readonly RECONNECT_BUDGET = 45_000;
    private static jobAbortControllers = new Map<string, AbortController>();
    private static reconnectingJobs = new Set<string>();
    private static readonly cancellationChannel = typeof window !== 'undefined' && 'BroadcastChannel' in window
        ? new BroadcastChannel('konomitv-offline-cancellation') : null;
    private static foregroundLockReleases = new Map<string, () => void>();
    /** Web Locks 非対応環境で、同一タブ内の start() 呼び出しを録画番組 ID ごとに直列化する */
    private static startMutexTailByVideoID = new Map<number, Promise<void>>();

    /** 前景保存中にタブを閉じようとしたときの警告 */
    private static readonly onBeforeUnload = (event: BeforeUnloadEvent): void => {
        event.preventDefault();
        event.returnValue = '';
    };

    static {
        this.cancellationChannel?.addEventListener('message', (event: MessageEvent<unknown>) => {
            if (typeof event.data === 'string') {
                this.jobAbortControllers.get(event.data)?.abort();
                this.notifyChange();
            }
        });
    }

    /**
     * Background Fetch を利用できるかを返す。
     * @returns 利用できる場合は true
     */
    static async isBackgroundFetchSupported(): Promise<boolean> {

        // Service Worker の登録がなければ Background Fetch の管理オブジェクトも取得できない
        if ('serviceWorker' in navigator === false) {
            return false;
        }
        const registration = await navigator.serviceWorker.getRegistration();
        return registration?.backgroundFetch !== undefined;
    }

    /**
     * 保存済み動画を取得する。
     * @returns 保存日時が新しい順の動画一覧
     */
    static async getVideos(): Promise<IOfflineVideo[]> {
        return await OfflineVideoStorage.getVideos();
    }

    /**
     * 録画番組 ID に対応する保存済み動画を取得する。
     * @param videoID 録画番組 ID
     * @returns 保存済み動画（存在しない場合は null）
     */
    static async getVideo(videoID: number): Promise<IOfflineVideo | null> {
        return await OfflineVideoStorage.getVideo(videoID);
    }

    /**
     * 実行中または完了した保存ジョブを取得する。
     * @returns 保存ジョブ一覧
     */
    static async getJobs(): Promise<IOfflineDownloadJob[]> {

        return await OfflineVideoStorage.getJobs();
    }

    static isReconnecting(jobID: string): boolean {
        return this.reconnectingJobs.has(jobID);
    }

    /**
     * 指定した録画番組の実行中保存ジョブを取得する。
     * @param videoID 録画番組 ID
     * @returns 実行中ジョブ (存在しない場合は null)
     */
    static async getActiveJobForVideo(videoID: number): Promise<IOfflineDownloadJob | null> {

        const jobs = await this.getJobs();
        return jobs.find(job =>
            job.video_id === videoID && ['Waiting', 'Downloading', 'Finalizing'].includes(job.state),
        ) ?? null;
    }

    /**
     * ページ内 Fetch で保存中のジョブがあるかを返す。
     * @returns 前景保存中なら true
     */
    static async hasActiveForegroundDownload(): Promise<boolean> {

        // IndexedDB は別タブとも共有されるため、このページが所有するロックだけを更新延期の判定に使う
        return this.foregroundLockReleases.size > 0;
    }

    /** 前回のページ終了後も実行中として残った前景保存ジョブを失敗状態へ回収する */
    static async recoverInterruptedForegroundDownloads(): Promise<void> {

        const jobs = await this.getJobs();
        if ('locks' in navigator) {
            for (const job of jobs) {
                if (job.background_fetch_id !== null || ['Waiting', 'Downloading', 'Finalizing'].includes(job.state) === false) continue;

                // Web Locks を取得できなければ別タブが保存中なので、共有 IndexedDB の状態へ触れない
                await navigator.locks.request(this.getForegroundLockName(job.job_id), {ifAvailable: true}, async (lock) => {
                    if (lock === null) return;
                    const latestJob = await OfflineVideoStorage.getJob(job.job_id);
                    if (latestJob === null || ['Waiting', 'Downloading', 'Finalizing'].includes(latestJob.state) === false) return;
                    await this.markJobFailed(latestJob.job_id, 'ページが閉じられたため、オフライン保存が中断されました。');
                });
            }
        } else {
            // Web Locks 非対応環境では所有タブを識別できないため、前景保存の実行中ジョブは起動時に失敗へ倒して断片を回収する
            for (const job of jobs) {
                if (job.background_fetch_id !== null || ['Waiting', 'Downloading', 'Finalizing'].includes(job.state) === false) continue;
                await this.markJobFailed(job.job_id, 'ページが閉じられたため、オフライン保存が中断されました。');
            }
        }
    }

    /**
     * 録画番組のオフライン保存を開始する。
     * @param program 保存する録画番組
     * @param quality 追加オプションを含む API 画質
     * @param useBackgroundFetch ダイアログでバックグラウンド保存が選ばれたか (未対応環境では前景 Fetch を使う)
     * @returns 作成した保存ジョブ
     */
    static async start(
        program: IRecordedProgram,
        quality: string,
        useBackgroundFetch: boolean = false,
        retryJobID?: string,
    ): Promise<IOfflineDownloadJob> {

        // Vue コンポーネントから渡される番組情報はリアクティブ Proxy のため、そのままでは IndexedDB の構造化複製に失敗する
        // API 由来の JSON データだけを保存時点のスナップショットへ変換し、Service Worker からも安全に読み出せる値へ固定する
        const programSnapshot = JSON.parse(JSON.stringify(program)) as IRecordedProgram;

        // 同一録画番組への並行 start() を直列化し、IndexedDB へジョブが載る前の重複開始を防ぐ
        return await this.withVideoStartLock(programSnapshot.id, async () => {
            const activeJob = await this.getActiveJobForVideo(programSnapshot.id);
            if (activeJob !== null) {
                throw new Error('この録画番組はすでにオフライン保存中です。');
            }

            // 進捗バーは目標ビットレート基準、空き容量判定は上限ビットレート基準で見積もる
            const estimatedSizeBytes = OfflineVideos.estimateJobSizeBytes(programSnapshot.recorded_video.duration, quality);
            if (estimatedSizeBytes === null) {
                throw new Error(`オフライン保存に対応していない画質です。(${quality})`);
            }
            const requiredStorageBytes = OfflineVideos.estimateRequiredStorageBytes(programSnapshot.recorded_video.duration, quality);
            if (requiredStorageBytes === null) {
                throw new Error(`オフライン保存に対応していない画質です。(${quality})`);
            }

            // ダイアログのスイッチがオンで、かつ Background Fetch API が使えるときだけバックグラウンド保存する
            // オフや未対応環境ではページ内 Fetch で保存し、複数本の同時ダウンロードを優先する
            const shouldUseBackgroundFetch = useBackgroundFetch === true && await this.isBackgroundFetchSupported() === true;
            if (retryJobID !== undefined && useBackgroundFetch && !shouldUseBackgroundFetch) {
                throw new Error('このブラウザではバックグラウンド保存を利用できません。');
            }

            // Background Fetch は一時応答と展開後キャッシュが併存するため、前景保存の2倍を空き容量判定へ使う
            const storageEstimate = await navigator.storage?.estimate() ?? {};
            const availableBytes = (storageEstimate.quota ?? 0) - (storageEstimate.usage ?? 0);
            const requiredBytes = requiredStorageBytes * (shouldUseBackgroundFetch === true ? 2 : 1);
            if (storageEstimate.quota !== undefined && availableBytes < requiredBytes) {
                throw new Error(`オフライン保存に必要な空き容量が不足しています。必要: ${Utils.formatBytes(requiredBytes)} / 空き: ${Utils.formatBytes(Math.max(0, availableBytes))}`);
            }

            // 同じ録画番組を保存し直す場合も、旧世代を消さず新しい世代へ書き込む
            const generationID = crypto.randomUUID();
            const jobID = crypto.randomUUID();
            const job: IOfflineDownloadJob = {
                job_id: jobID,
                video_id: programSnapshot.id,
                generation_id: generationID,
                program: programSnapshot,
                quality,
                state: 'Waiting',
                estimated_size_bytes: estimatedSizeBytes,
                downloaded_bytes: 0,
                background_fetch_id: shouldUseBackgroundFetch === true ? `konomitv-offline-${jobID}` : null,
                error: null,
            };
            const controller = new AbortController();
            this.jobAbortControllers.set(jobID, controller);
            let releaseForegroundLock: (() => void) | null = null;
            let isRegistered = false;
            let isHandedOff = false;
            try {
                if (!shouldUseBackgroundFetch) {
                    releaseForegroundLock = await this.acquireForegroundLock(jobID);
                }
                await OfflineVideoStorage.putJobIfVideoIdle(job, retryJobID);
                isRegistered = true;
                await this.prepareAssets(job, controller.signal);
                controller.signal.throwIfAborted();

                if (job.background_fetch_id !== null) {
                    const registration = await navigator.serviceWorker.getRegistration();
                    const manager = registration?.backgroundFetch;
                    if (manager === undefined) {
                        throw new Error('Background Fetch API を利用できません。');
                    }
                    await this.updateJob(job);
                    controller.signal.throwIfAborted();
                    const request = new Request(`${Utils.api_base_url}/streams/video/${job.video_id}/${quality}/offline-stream`);
                    const backgroundFetch = await manager.fetch(job.background_fetch_id, [request], {
                        title: `${programSnapshot.title}をオフライン保存`,
                        icons: [{src: '/assets/images/icons/icon-192px.png', sizes: '192x192', type: 'image/png'}],
                    });
                    try {
                        const latestJob = await OfflineVideoStorage.getJob(jobID);
                        if (latestJob === null || latestJob.state === 'Cancelled' || latestJob.state === 'Failed') {
                            throw new Error('オフライン保存はキャンセルされました。');
                        }
                    } catch (error) {
                        await backgroundFetch.abort();
                        throw error;
                    }
                    this.notifyChange();
                } else {
                    isHandedOff = true;
                    void this.runForegroundDownload(job, controller, releaseForegroundLock).catch((error: unknown) => {
                        console.error('[OfflineVideos] Failed to finish offline download cleanup:', error);
                    });
                }
                return job;
            } catch (error) {
                if (isRegistered) {
                    const wasCancelled = controller.signal.aborted;
                    controller.abort();
                    // 準備処理の終了後に状態を確定し、未完成データを一度だけ回収する。
                    if (wasCancelled) {
                        await OfflineVideoStorage.transitionActiveJobToTerminalState(jobID, 'Cancelled', null);
                    } else {
                        await OfflineVideoStorage.transitionActiveJobToTerminalState(
                            jobID, 'Failed', error instanceof Error ? error.message : 'オフライン保存を開始できませんでした。',
                        );
                    }
                    await this.deleteIncompleteGeneration(job);
                    this.notifyChange();
                }
                throw error;
            } finally {
                if (!isHandedOff) {
                    this.jobAbortControllers.delete(jobID);
                    releaseForegroundLock?.();
                }
            }
        });
    }

    /** 失敗したジョブを同じ画質と保存方式で先頭からやり直す。 */
    static async retry(jobID: string): Promise<IOfflineDownloadJob> {
        const failedJob = await OfflineVideoStorage.getJob(jobID);
        if (failedJob?.state !== 'Failed') {
            throw new Error('やり直す保存が見つかりませんでした。');
        }
        const program = await Videos.fetchVideo(failedJob.video_id);
        if (program === null) {
            throw new Error('録画番組の情報を取得できませんでした。');
        }
        return await this.start(program, failedJob.quality, failedJob.background_fetch_id !== null, jobID);
    }

    private static async prepareAssets(job: IOfflineDownloadJob, signal: AbortSignal): Promise<void> {
        const cache = await OfflineVideoStorage.openCache();
        const baseURL = OfflineVideoStorage.getGenerationBaseURL(job.video_id, job.generation_id);
        const assets = [
            {source: Videos.getThumbnailURL(job.program), name: 'thumbnail.webp'},
            {source: `${Utils.api_base_url}/videos/${job.video_id}/thumbnail/tiled`, name: 'thumbnail-tiled.webp'},
        ];
        if (job.program.channel !== null) {
            assets.push({source: `${Utils.api_base_url}/channels/${job.program.channel.id}/logo`, name: 'channel-logo'});
        }

        // 終了時の世代回収より先にすべての cache.put() が決着するように待つ。
        const results = await Promise.allSettled(assets.map(asset =>
            this.cacheOptionalAsset(cache, asset.source, `${baseURL}/assets/${asset.name}`, signal),
        ));
        signal.throwIfAborted();
        for (const result of results) {
            if (result.status === 'rejected') {
                throw result.reason;
            }
        }
        await this.updateJob(job);

        // 実況は時間がかかるため、保存開始と並行で受信する。
        void (async () => {
            const destination = `${baseURL}/assets/jikkyo.json`;
            const response = await fetch(new Request(`${Utils.api_base_url}/videos/${job.video_id}/jikkyo`, {signal}));
            if (!response.ok) return;

            // 受信を待つ間に終了した保存の状態を確認する。
            const latestJob = await OfflineVideoStorage.getJob(job.job_id);
            if (latestJob === null || latestJob.generation_id !== job.generation_id ||
                ['Failed', 'Cancelled'].includes(latestJob.state)) {
                return;
            }
            await cache.put(destination, response);

            // 書き込み中に終了した場合は、遅れて保存された実況データも回収する。
            const jobAfterWrite = await OfflineVideoStorage.getJob(job.job_id);
            if (jobAfterWrite === null || jobAfterWrite.generation_id !== job.generation_id ||
                ['Failed', 'Cancelled'].includes(jobAfterWrite.state)) {
                await cache.delete(destination);
            }
        })().catch((error: unknown) => {
            console.warn('[OfflineVideos] Failed to cache offline comments:', error);
        });
    }

    private static async cacheOptionalAsset(cache: Cache, source: string, destination: string, signal: AbortSignal): Promise<void> {
        let response: Response | undefined;
        try {
            response = await fetch(source, {signal});
            if (!response.ok) {
                console.warn('[OfflineVideos] Optional offline asset returned HTTP', response.status);
                return;
            }
            // cache.put() が本文を読み切るまで、ジョブのキャンセルを Fetch に伝える。
            try {
                await cache.put(destination, response);
            } catch (error) {
                signal.throwIfAborted();
                console.warn('[OfflineVideos] Failed to cache an optional offline asset:', error);
            }
        } catch (error) {
            signal.throwIfAborted();
            if (error instanceof TypeError || (error instanceof DOMException && error.name === 'NetworkError')) {
                console.warn('[OfflineVideos] Failed to download an optional offline asset:', error);
                return;
            }
            throw error;
        } finally {
            // HTTP エラーや書き込み開始前の失敗では、未読の本文のダウンロードを残さない。
            if (response !== undefined && !response.bodyUsed && !signal.aborted) {
                await response.body?.cancel();
            }
        }
    }

    private static async runForegroundDownload(
        job: IOfflineDownloadJob,
        controller: AbortController,
        releaseForegroundLock: (() => void) | null,
    ): Promise<void> {
        const signal = controller.signal;
        const sessionID = Array.from(crypto.getRandomValues(new Uint8Array(16)), value =>
            value.toString(16).padStart(2, '0'),
        ).join('');
        const url = `${Utils.api_base_url}/streams/video/${job.video_id}/${job.quality}/offline-sessions/${sessionID}`;
        let sessionInfo: IOfflineVideoSessionInfo | undefined;
        let wakeLock: WakeLockSentinel | undefined;
        let releaseActivity: (() => void) | null = null;
        try {
            // 前景保存中だけ、画面の消灯とバックグラウンドでのページ凍結を抑える。
            if ('wakeLock' in navigator) {
                try {
                    wakeLock = await navigator.wakeLock.request('screen');
                } catch (error) {
                    console.warn('[OfflineVideos] Failed to acquire a screen wake lock:', error);
                }
            }
            try {
                signal.throwIfAborted();
                releaseActivity = await this.acquireForegroundActivityProtection(signal);
            } catch (error) {
                signal.throwIfAborted();
                console.warn('[OfflineVideos] Failed to acquire foreground activity protection:', error);
            }
            sessionInfo = await this.createSession(url, signal);
            signal.throwIfAborted();
            if (!this.matchesJob(sessionInfo, job)) {
                throw new Error('オフライン保存の開始応答が録画番組や画質と一致しません。');
            }

            const cache = await OfflineVideoStorage.openCache();
            const progress: IOfflineDownloadProgress = {segmentDurations: [], sizeBytes: 0};
            let connectionID = 0;
            let reconnectCount = 0;
            let reconnectDeadline = 0;
            while (true) {
                const nextSequence = progress.segmentDurations.length;
                const connection = this.linkAbortController(signal);
                let checkpointTask: Promise<void> | undefined;
                let checkpointError: unknown;
                let reconnectError: OfflineDownloadNetworkError;
                try {
                    signal.throwIfAborted();
                    if (reconnectCount > 0 && performance.now() >= reconnectDeadline) {
                        throw new Error('通信を再開できませんでした。');
                    }
                    connectionID++;
                    const response = await this.openSessionStream(
                        url, sessionInfo, nextSequence, connectionID, connection.controller,
                    );
                    job.state = 'Downloading';
                    await this.updateJob(job);
                    checkpointTask = this.sendCheckpoints(url, sessionInfo, connectionID, progress, connection.controller.signal)
                        .catch((error: unknown) => {
                            if (connection.controller.signal.aborted === false) {
                                checkpointError = error;
                                connection.controller.abort();
                            }
                        });
                    const metadata = await this.receiveSegments(job, response, cache, progress, {
                        sessionID,
                        info: sessionInfo,
                        controller: connection.controller,
                    });
                    connection.controller.abort();
                    await checkpointTask;
                    if (checkpointError !== undefined) throw checkpointError;
                    signal.throwIfAborted();
                    await this.completeDownload(job, cache, metadata, progress);
                    return;
                } catch (error) {
                    signal.throwIfAborted();
                    if (checkpointError !== undefined) throw checkpointError;
                    if (error instanceof OfflineDownloadNetworkError === false) throw error;
                    reconnectError = error;
                    if (progress.segmentDurations.length > nextSequence) {
                        reconnectCount = 0;
                    }
                    if (reconnectCount === 0) {
                        reconnectDeadline = performance.now() + this.RECONNECT_BUDGET;
                    }
                } finally {
                    // 次の GET の前に旧接続の制御要求を中断する。本文 reader は receiveSegments() が回収する。
                    connection.controller.abort();
                    connection.dispose();
                    await checkpointTask;
                }

                job.downloaded_bytes = progress.sizeBytes;
                this.setReconnecting(job.job_id, true);
                await this.updateJob(job);
                await this.waitToReconnect(reconnectError, reconnectCount, reconnectDeadline, signal);
                reconnectCount++;
            }
        } catch (error) {
            try {
                if (signal.aborted) {
                    await OfflineVideoStorage.transitionActiveJobToTerminalState(job.job_id, 'Cancelled', null);
                } else {
                    console.error('[OfflineVideos] Foreground offline download failed:', error);
                    await OfflineVideoStorage.transitionActiveJobToTerminalState(
                        job.job_id, 'Failed', error instanceof Error ? error.message : 'オフライン保存に失敗しました。',
                    );
                }
            } finally {
                // 書き込みが決着してから回収し、キャンセル後に保存された断片も削除する。
                await this.deleteIncompleteGeneration(job);
                this.notifyChange();
            }
        } finally {
            this.setReconnecting(job.job_id, false);
            this.jobAbortControllers.delete(job.job_id);
            releaseActivity?.();
            releaseForegroundLock?.();
            void wakeLock?.release().catch((error: unknown) => {
                console.warn('[OfflineVideos] Failed to release the screen wake lock:', error);
            });
            if (sessionInfo !== undefined) {
                const cleanupController = new AbortController();
                const timeout = setTimeout(() => cleanupController.abort(), 5000);
                try {
                    const response = await this.fetchResponse(url, {
                        method: 'DELETE',
                        headers: {'X-KonomiTV-Offline-Generation': sessionInfo.generation_token},
                        signal: cleanupController.signal,
                    });
                    if (!response.ok) console.warn('[OfflineVideos] Offline download cleanup returned HTTP', response.status);
                } catch (error) {
                    console.warn('[OfflineVideos] Could not notify the server that offline saving ended:', error);
                } finally {
                    clearTimeout(timeout);
                    cleanupController.abort();
                }
            }
        }
    }

    private static async createSession(url: string, signal: AbortSignal): Promise<IOfflineVideoSessionInfo> {
        let retryCount = 0;
        let retryDeadline = 0;
        while (true) {
            signal.throwIfAborted();
            if (retryCount > 0 && performance.now() >= retryDeadline) {
                throw new Error('オフライン保存の接続を開始できませんでした。');
            }
            const {controller, dispose} = this.linkAbortController(signal);
            const timeout = setTimeout(() => controller.abort(), 30_000);
            let retryError: OfflineDownloadNetworkError;
            try {
                const response = await this.fetchResponse(url, {method: 'PUT', signal: controller.signal});
                await this.checkSessionResponse(response, controller.signal);
                if (response.status !== 201 && response.status !== 200) {
                    throw new Error('オフライン保存の開始応答が不正です。');
                }
                const info: unknown = await this.readSessionJSON(response, controller.signal);
                if (!this.isSessionInfo(info)) {
                    throw new Error('オフライン保存の開始応答が不正です。');
                }
                return info;
            } catch (error) {
                signal.throwIfAborted();
                if (error instanceof OfflineDownloadNetworkError === false) throw error;
                retryError = error;
            } finally {
                clearTimeout(timeout);
                controller.abort();
                dispose();
            }
            if (retryError.status === 429) {
                await this.waitForDelay(Math.max(5000, retryError.retryAfterMilliseconds), signal);
                retryCount = 0;
                continue;
            }
            if (retryCount === 0) retryDeadline = performance.now() + this.RECONNECT_BUDGET;
            await this.waitToReconnect(retryError, retryCount, retryDeadline, signal);
            retryCount++;
        }
    }

    private static async openSessionStream(
        url: string,
        info: IOfflineVideoSessionInfo,
        nextSequence: number,
        connectionID: number,
        controller: AbortController,
    ): Promise<Response> {
        const timeout = setTimeout(() => controller.abort(new OfflineDownloadNetworkError('オフライン保存の接続がタイムアウトしました。')), 30_000);
        try {
            const response = await this.fetchResponse(`${url}?next_sequence=${nextSequence}&connection_id=${connectionID}`, {
                headers: {'X-KonomiTV-Offline-Generation': info.generation_token},
                signal: controller.signal,
            });
            controller.signal.throwIfAborted();
            await this.checkSessionResponse(response, controller.signal);
            if (response.status !== 200) {
                throw new Error('オフライン保存の配信応答が不正です。');
            }
            return response;
        } finally {
            clearTimeout(timeout);
        }
    }

    private static async sendCheckpoints(
        url: string,
        info: IOfflineVideoSessionInfo,
        connectionID: number,
        progress: IOfflineDownloadProgress,
        signal: AbortSignal,
    ): Promise<void> {
        let lastSentAt = performance.now();
        let lastAcknowledged = progress.segmentDurations.length;
        while (true) {
            await this.waitForDelay(1000, signal);
            const nextSequence = progress.segmentDurations.length;
            const interval = nextSequence > lastAcknowledged ? 3000 : 10_000;
            if (performance.now() - lastSentAt < interval) continue;

            // 同じ位置でも生存通知を送る。通信に失敗したときは最新の位置を次回送信する。
            lastSentAt = performance.now();
            const {controller, dispose} = this.linkAbortController(signal);
            const timeout = setTimeout(() => controller.abort(), 10_000);
            try {
                const response = await this.fetchResponse(`${url}/checkpoint`, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-KonomiTV-Offline-Generation': info.generation_token,
                    },
                    body: JSON.stringify({connection_id: connectionID, next_sequence: nextSequence}),
                    signal: controller.signal,
                });
                await this.checkSessionResponse(response, controller.signal);
                const result = await this.readSessionJSON(response, controller.signal) as {connection_id?: number; next_sequence?: number} | null;
                if (result?.connection_id !== connectionID || typeof result.next_sequence !== 'number' ||
                    !Number.isInteger(result.next_sequence) || result.next_sequence < nextSequence ||
                    result.next_sequence > progress.segmentDurations.length) {
                    throw new Error('オフライン保存の進捗確認に失敗しました。');
                }
                lastAcknowledged = result.next_sequence;
            } catch (error) {
                signal.throwIfAborted();
                if (error instanceof OfflineDownloadNetworkError === false) throw error;
            } finally {
                clearTimeout(timeout);
                controller.abort();
                dispose();
            }
        }
    }

    private static async fetchResponse(url: string, options: RequestInit): Promise<Response> {
        const request = new Request(url, {...options, redirect: 'error'});
        try {
            return await fetch(request);
        } catch (error) {
            if (options.signal?.aborted || error instanceof TypeError || (error instanceof DOMException && error.name === 'NetworkError')) {
                throw new OfflineDownloadNetworkError('オフライン保存の通信が途切れました。');
            }
            throw error;
        }
    }

    private static async checkSessionResponse(response: Response, signal: AbortSignal): Promise<void> {
        if (response.ok) return;
        if ([408, 429, 502, 503, 504].includes(response.status)) {
            const retryAfter = response.headers.get('Retry-After');
            let milliseconds = 0;
            if (retryAfter !== null) {
                const seconds = Number(retryAfter);
                milliseconds = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(retryAfter) - Date.now();
            }
            throw new OfflineDownloadNetworkError(
                `オフライン保存の通信に失敗しました。(HTTP ${response.status})`,
                Number.isFinite(milliseconds) ? Math.max(0, milliseconds) : 0,
                response.status,
            );
        }
        if (response.status === 410) {
            throw new Error('オフライン保存を再開できませんでした。');
        }
        if (response.status === 409) {
            type ErrorResponse = {detail?: {code?: string}};
            let errorResponse: ErrorResponse | null = null;
            try {
                errorResponse = await this.readSessionJSON(response, signal) as ErrorResponse | null;
            } catch (error) {
                console.warn('[OfflineVideos] Could not read the offline download error response:', error);
            }
            if (errorResponse?.detail?.code === 'recording_changed' || errorResponse?.detail?.code === 'recording_unavailable') {
                throw new Error('録画ファイルが変わったか、現在は保存できない状態です。');
            }
            throw new Error('サーバーがオフライン保存の続行を拒否しました。');
        }
        throw new Error(`オフライン保存に失敗しました。(HTTP ${response.status})`);
    }

    private static async readSessionJSON(response: Response, signal: AbortSignal): Promise<unknown> {
        try {
            return await response.json() as unknown;
        } catch (error) {
            if (signal.aborted || error instanceof TypeError || (error instanceof DOMException && error.name === 'NetworkError')) {
                throw new OfflineDownloadNetworkError('オフライン保存の応答を受信できませんでした。');
            }
            if (error instanceof SyntaxError) {
                throw new Error('オフライン保存の応答が不正です。');
            }
            throw error;
        }
    }

    private static isSessionInfo(info: unknown): info is IOfflineVideoSessionInfo {
        if (typeof info !== 'object' || info === null) return false;
        const data = info as Partial<IOfflineVideoSessionInfo>;
        return data.protocol_version === 2 &&
            typeof data.generation_token === 'string' && /^[0-9a-f]{32}$/.test(data.generation_token);
    }

    private static matchesJob(metadata: IOfflineVideoStreamMetadata, job: IOfflineDownloadJob): boolean {
        return metadata.video_id === job.video_id && metadata.file_hash === job.program.recorded_video.file_hash &&
            metadata.quality === job.quality && Number.isFinite(metadata.duration_seconds) && metadata.duration_seconds > 0;
    }

    private static linkAbortController(signal: AbortSignal): {controller: AbortController; dispose: () => void} {
        const controller = new AbortController();
        const onAbort = () => controller.abort();
        if (signal.aborted) {
            controller.abort();
        } else {
            signal.addEventListener('abort', onAbort, {once: true});
        }
        return {controller, dispose: () => signal.removeEventListener('abort', onAbort)};
    }

    private static async waitForDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
        signal.throwIfAborted();
        await new Promise<void>((resolve, reject) => {
            const onAbort = () => {
                clearTimeout(timeout);
                reject(signal.reason);
            };
            const timeout = setTimeout(() => {
                signal.removeEventListener('abort', onAbort);
                resolve();
            }, milliseconds);
            signal.addEventListener('abort', onAbort, {once: true});
        });
    }

    private static async waitToReconnect(
        error: OfflineDownloadNetworkError,
        reconnectCount: number,
        deadline: number,
        signal: AbortSignal,
    ): Promise<void> {
        const delay = this.RECONNECT_DELAYS[reconnectCount];
        if (delay === undefined) throw new Error('通信を再開できませんでした。');
        const wait = Math.max(delay * (0.8 + Math.random() * 0.4), error.retryAfterMilliseconds);
        if (performance.now() + wait >= deadline) throw new Error('通信を再開できませんでした。');
        await this.waitForDelay(wait, signal);
    }

    private static setReconnecting(jobID: string, isReconnecting: boolean): void {
        if (isReconnecting === this.reconnectingJobs.has(jobID)) return;
        if (isReconnecting) {
            this.reconnectingJobs.add(jobID);
        } else {
            this.reconnectingJobs.delete(jobID);
        }
        this.notifyChange();
    }

    private static async updateJob(job: IOfflineDownloadJob): Promise<void> {
        if (await OfflineVideoStorage.updateActiveJob(job) === false) {
            throw new Error('オフライン保存はすでに終了しています。');
        }
    }

    private static async deleteIncompleteGeneration(job: IOfflineDownloadJob): Promise<void> {
        const savedVideo = await OfflineVideoStorage.getStoredVideo(job.video_id);
        if (savedVideo?.generation_id !== job.generation_id) {
            await OfflineVideoStorage.deleteGeneration(job.video_id, job.generation_id);
        }
    }

    /**
     * Background Fetch または前景 Fetch の応答を CacheStorage へ展開する。
     * @param jobID 保存ジョブ ID
     * @param response オフライン保存 API の応答
     */
    static async finalizeResponse(jobID: string, response: Response): Promise<void> {
        const job = await OfflineVideoStorage.getJob(jobID);
        if (job === null) {
            throw new Error('保存ジョブが見つかりません。');
        }
        try {
            job.state = 'Downloading';
            job.downloaded_bytes = 0;
            await this.updateJob(job);
            const cache = await OfflineVideoStorage.openCache();
            const progress: IOfflineDownloadProgress = {segmentDurations: [], sizeBytes: 0};
            const metadata = await this.receiveSegments(job, response, cache, progress);
            await this.completeDownload(job, cache, metadata, progress);
        } catch (error) {
            // 実行中のジョブは Service Worker が失敗を確定して回収する。
            // 先にキャンセルや失敗が確定していた場合は、その後に書き込まれた断片をここで回収する。
            try {
                const latestJob = await OfflineVideoStorage.getJob(jobID);
                if (latestJob === null || latestJob.state === 'Cancelled' || latestJob.state === 'Failed') {
                    await this.deleteIncompleteGeneration(job);
                }
            } catch (cleanupError) {
                console.warn('[OfflineVideos] Failed to clean up the offline video download:', cleanupError);
            }
            throw error;
        }
    }

    private static async receiveSegments(
        job: IOfflineDownloadJob,
        response: Response,
        cache: Cache,
        progress: IOfflineDownloadProgress,
        connection?: IOfflineVideoConnection,
    ): Promise<IOfflineVideoStreamMetadata> {
        if (response.body === null) throw new Error('オフライン保存の応答本体がありません。');
        const reader = response.body.getReader();
        const signal = connection?.controller.signal;
        const baseURL = OfflineVideoStorage.getGenerationBaseURL(job.video_id, job.generation_id);
        let chunk: Uint8Array | undefined;
        let chunkOffset = 0;
        let lastPersistedProgressAt = performance.now();
        let lastPersistedProgressBytes = job.downloaded_bytes;

        /** 必要な領域へ直接コピーし、読み取り待ちの時間だけを通信の無受信時間として扱う。 */
        const readBytes = async (length: number, isSegmentBody = false): Promise<Uint8Array> => {
            const output = new Uint8Array(length);
            let offset = 0;
            while (offset < length) {
                signal?.throwIfAborted();
                if (chunk === undefined || chunkOffset === chunk.byteLength) {
                    const timeout = connection === undefined ? undefined : setTimeout(() => {
                        connection.controller.abort(new OfflineDownloadNetworkError('オフライン保存の受信がタイムアウトしました。'));
                    }, 30_000);
                    let result: ReadableStreamReadResult<Uint8Array>;
                    try {
                        result = await reader.read();
                    } catch (error) {
                        if (connection !== undefined && (
                            signal?.aborted || error instanceof TypeError || (error instanceof DOMException && error.name === 'NetworkError')
                        )) {
                            throw new OfflineDownloadNetworkError('オフライン保存の通信が途切れました。');
                        }
                        throw error;
                    } finally {
                        clearTimeout(timeout);
                    }
                    if (result.done) {
                        throw new OfflineDownloadNetworkError('オフライン保存データが途中で終了しました。');
                    }
                    chunk = result.value;
                    chunkOffset = 0;
                }
                const copyLength = Math.min(length - offset, chunk.byteLength - chunkOffset);
                output.set(chunk.subarray(chunkOffset, chunkOffset + copyLength), offset);
                offset += copyLength;
                chunkOffset += copyLength;
                if (isSegmentBody) {
                    job.downloaded_bytes = progress.sizeBytes + offset;
                    const currentTime = performance.now();
                    if (job.downloaded_bytes - lastPersistedProgressBytes >= 1024 * 1024 || currentTime - lastPersistedProgressAt >= 250) {
                        await this.updateJob(job);
                        lastPersistedProgressAt = currentTime;
                        lastPersistedProgressBytes = job.downloaded_bytes;
                    }
                }
            }
            return output;
        };

        try {
            const magic = await readBytes(this.STREAM_MAGIC.byteLength);
            if (magic.some((value, index) => value !== this.STREAM_MAGIC[index])) {
                throw new Error('オフライン保存データの形式を認識できません。');
            }

            const metadataLength = new DataView((await readBytes(4)).buffer).getUint32(0);
            if (metadataLength === 0 || metadataLength > 1024 * 1024) {
                throw new Error('オフライン保存データのメタデータ長が不正です。');
            }
            const metadata = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(await readBytes(metadataLength))) as
                IOfflineVideoStreamMetadata & Partial<IOfflineVideoSessionInfo> & {session_id?: string; next_sequence?: number};
            if (metadata === null || !this.matchesJob(metadata, job)) {
                throw new Error('オフライン保存データが録画番組や画質と一致しません。');
            }
            let maxSegmentBytes = 128 * 1024 * 1024;
            if (connection !== undefined) {
                const {info} = connection;
                if (metadata.session_id !== connection.sessionID || metadata.next_sequence !== progress.segmentDurations.length ||
                    !this.isSessionInfo(metadata) || metadata.generation_token !== info.generation_token ||
                    metadata.duration_seconds !== info.duration_seconds) {
                    throw new Error('オフライン保存の配信応答が開始時の情報と一致しません。');
                }
                maxSegmentBytes = this.MAX_SEGMENT_BYTES;
                this.setReconnecting(job.job_id, false);
            }

            while (true) {
                // keepalive と終端は 8 バイト、映像レコードは 12 バイトのヘッダーを持つ。
                const record = new DataView((await readBytes(8)).buffer);
                const sequence = record.getUint32(0);
                const value = record.getUint32(4);
                if (sequence === 0xffffffff) {
                    if (value === 0 || value !== progress.segmentDurations.length) {
                        throw new Error('オフライン保存データのセグメント数が一致しません。');
                    }
                    return metadata;
                }
                if (sequence === 0xfffffffe && connection !== undefined && value === 0) {
                    continue;
                }
                if (sequence !== progress.segmentDurations.length || value === 0) {
                    throw new Error('オフライン保存データのセグメントヘッダーが不正です。');
                }
                const segmentLength = new DataView((await readBytes(4)).buffer).getUint32(0);
                if (segmentLength === 0 || segmentLength > maxSegmentBytes) {
                    throw new Error('オフライン保存データのセグメントの長さが不正です。');
                }
                const segmentData = await readBytes(segmentLength, true);
                signal?.throwIfAborted();
                await cache.put(`${baseURL}/segments/${sequence}.ts`, new Response(segmentData, {
                    headers: {'Content-Type': 'video/mp2t'},
                }));
                // 通常 Fetch ではジョブ状態の確定を待ってから、次に要求する番号を進める。
                if (connection !== undefined) {
                    await this.updateJob(job);
                }
                progress.segmentDurations.push(value / 1000);
                progress.sizeBytes += segmentLength;
            }
        } finally {
            await reader.cancel().catch((error: unknown) => {
                if (signal?.aborted !== true) {
                    console.warn('[OfflineVideos] Failed to cancel the offline download response reader:', error);
                }
            });
            reader.releaseLock();
        }
    }

    private static async completeDownload(
        job: IOfflineDownloadJob,
        cache: Cache,
        metadata: IOfflineVideoStreamMetadata,
        progress: IOfflineDownloadProgress,
    ): Promise<void> {
        job.state = 'Finalizing';
        await this.updateJob(job);
        const baseURL = OfflineVideoStorage.getGenerationBaseURL(job.video_id, job.generation_id);
        const {segmentDurations, sizeBytes} = progress;
        const targetDuration = Math.ceil(segmentDurations.reduce((maximum, duration) => Math.max(maximum, duration), 0));
        let playlist = '#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-PLAYLIST-TYPE:VOD\n';
        playlist += `#EXT-X-TARGETDURATION:${targetDuration}\n`;
        segmentDurations.forEach((duration, sequence) => {
            playlist += `#EXTINF:${duration.toFixed(3)},\nsegments/${sequence}.ts\n`;
        });
        playlist += '#EXT-X-ENDLIST\n';
        await cache.put(`${baseURL}/playlist.m3u8`, new Response(playlist, {
            headers: {'Content-Type': 'application/vnd.apple.mpegurl'},
        }));

        // 完成した動画とジョブを同じトランザクションで切り替えてから旧データを回収する。
        const previousVideo = await OfflineVideoStorage.getStoredVideo(job.video_id);
        const video: IOfflineVideo = {
            video_id: job.video_id,
            generation_id: job.generation_id,
            file_hash: metadata.file_hash,
            program: job.program,
            quality: job.quality,
            size_bytes: sizeBytes,
            segment_count: segmentDurations.length,
            saved_at: Date.now(),
        };
        if (!await OfflineVideoStorage.completeJob(job, video)) {
            throw new Error('オフライン保存はすでに終了しています。');
        }
        if (previousVideo !== null && previousVideo.generation_id !== video.generation_id) {
            await OfflineVideoStorage.deleteGeneration(previousVideo.video_id, previousVideo.generation_id).catch((error: unknown) => {
                console.warn('[OfflineVideos] Failed to delete the previous offline video data:', error);
            });
        }
        this.notifyChange();
    }

    /**
     * Service Worker で完了できなかった保存ジョブを失敗状態へ更新する。
     * @param jobID 保存ジョブ ID
     * @param error 利用者へ表示する失敗理由
     */
    static async markJobFailed(jobID: string, error: string): Promise<void> {

        // 状態遷移に勝った失敗処理だけが断片を削除し、確定済みの保存世代には触れない
        const job = await OfflineVideoStorage.transitionActiveJobToTerminalState(jobID, 'Failed', error);
        if (job === null) return;

        // 失敗理由は IndexedDB へ確定済みなので、断片削除の成否にかかわらず一覧から確認できる状態を保つ
        try {
            await OfflineVideoStorage.deleteGeneration(job.video_id, job.generation_id);
        } catch (deleteError) {
            console.warn('[OfflineVideos] Failed to delete incomplete offline video data:', deleteError);
        }
        this.notifyChange();
    }

    /**
     * Service Worker で中止された保存ジョブをキャンセル状態へ更新する。
     * @param jobID 保存ジョブ ID
     */
    static async markJobCancelled(jobID: string): Promise<void> {

        // Background Fetch の完了と中止が競合しても、先に確定した終端状態を保持する
        const job = await OfflineVideoStorage.transitionActiveJobToTerminalState(jobID, 'Cancelled', null);
        if (job === null) return;
        await OfflineVideoStorage.deleteGeneration(job.video_id, job.generation_id);
        this.notifyChange();
    }

    /**
     * 保存開始時に指定する API 画質文字列を組み立てる。
     * @param baseQuality 1080p や 720p などの基底画質
     * @param options 通信節約モードなどの付加オプション
     * @returns API 画質
     */
    static buildAPIQuality(baseQuality: string, options: {
        isDataSaverMode: boolean;
        isHEVCSupported: boolean;
        isHEVC10bitSupported: boolean;
        is24fpsMode: boolean;
    }): string {

        let apiQuality = baseQuality;
        if (options.isDataSaverMode === true && options.isHEVCSupported === true) {
            apiQuality += '-hevc';
            if (options.isHEVC10bitSupported === true) {
                apiQuality += '-10bit';
            }
        }
        if (options.is24fpsMode === true && baseQuality !== '1080p-60fps') {
            apiQuality += '-24fps';
        }
        return apiQuality;
    }

    /**
     * 保存ジョブの進捗計算向けに、VBR の実効平均ビットレートから保存容量を見積もる。
     * @param durationSeconds 番組尺 (秒)
     * @param apiQuality API 画質
     * @returns 見積もりバイト数。未対応画質なら null
     */
    static estimateJobSizeBytes(durationSeconds: number, apiQuality: string): number | null {

        const effectiveBitrates = OfflineVideos.getEffectiveStreamBitrates(apiQuality);
        if (effectiveBitrates.total_bitrate_kbps === 0) return null;

        const effectiveBitrateBps = effectiveBitrates.total_bitrate_kbps * 1000;
        return Math.ceil((effectiveBitrateBps * durationSeconds / 8) * OfflineVideos.JOB_PROGRESS_SIZE_MARGIN);
    }

    /**
     * 空き容量判定向けに、server/app/constants.py の video_bitrate_max + audio_bitrate から保存容量を見積もる。
     * @param durationSeconds 番組尺 (秒)
     * @param apiQuality API 画質
     * @returns 見積もりバイト数。未対応画質なら null
     */
    static estimateRequiredStorageBytes(durationSeconds: number, apiQuality: string): number | null {

        const bitrates = OfflineVideos.getStreamQualityBitrates(apiQuality);
        if (bitrates.video_bitrate_max_kbps === 0) return null;

        const maxBitrateBps = (bitrates.video_bitrate_max_kbps + bitrates.audio_bitrate_kbps) * 1000;
        return Math.ceil((maxBitrateBps * durationSeconds / 8) * OfflineVideos.REQUIRED_STORAGE_SIZE_MARGIN);
    }

    /**
     * 表示向けに、VBR の実効平均ビットレートから保存容量を見積もる。
     * @param durationSeconds 番組尺 (秒)
     * @param apiQuality API 画質
     * @returns 見積もりバイト数
     */
    static estimateDisplaySizeBytes(durationSeconds: number, apiQuality: string): number {

        const effectiveBitrates = OfflineVideos.getEffectiveStreamBitrates(apiQuality);
        return Math.ceil(effectiveBitrates.total_bitrate_kbps * 1000 * durationSeconds / 8);
    }

    /**
     * オフライン保存容量を利用者向けに整形する。
     * @param bytes バイト数
     * @param approximate 見積もり表示なら true
     * @returns 約 2.2GB や 340MB などの文字列
     */
    static formatOfflineSize(bytes: number, approximate: boolean): string {

        const formatted = OfflineVideos.formatOfflineSizeValue(bytes, approximate);
        return approximate === true ? `約${formatted}` : formatted;
    }

    /**
     * オフライン保存容量の単位付き文字列を返す。
     * 見積もり表示では GB 以上は小数1桁、MB は10MB刻みに丸める。
     * 実測表示では GB 以上は小数2桁、MB は1MB単位の整数、それ未満は通常の自動単位切り替え。
     * @param bytes バイト数
     * @param approximate 見積もり表示なら true
     * @returns 2.2GB や 340MB などの文字列
     */
    private static formatOfflineSizeValue(bytes: number, approximate: boolean): string {

        const oneMB = 1024 * 1024;
        const oneGB = oneMB * 1024;

        // GB 以上は見積もりなら粗く、実測なら小数2桁まで表示する
        if (bytes >= oneGB) {
            return Utils.formatBytes(bytes, approximate === true ? 1 : 2);
        }

        if (bytes >= oneMB) {
            const megabytes = bytes / oneMB;
            // 見積もりだけ 344MB のような細かい値を避け、340MB のように10MB刻みへ丸める
            if (approximate === true) {
                const roundedMegabytes = Math.round(megabytes / 10) * 10;
                return `${roundedMegabytes}MB`;
            }
            // 実測値は1MB単位でそのまま表示する
            return `${Math.round(megabytes)}MB`;
        }

        return Utils.formatBytes(bytes, approximate === true ? 1 : 0);
    }

    /**
     * 画質設定画面と同じ形式で平均ビットレートを返す。
     * @param apiQuality API 画質
     * @returns 4.9Mbps などの文字列
     */
    static formatAverageBitrateLabel(apiQuality: string): string {

        const effectiveBitrates = OfflineVideos.getEffectiveStreamBitrates(apiQuality);
        const averageMbps = effectiveBitrates.total_bitrate_kbps / 1000;
        return `${averageMbps.toFixed(1)}Mbps`;
    }

    /**
     * 保存画質セレクタ向けの表示ラベルを返す。
     * @param baseQuality 1080p や 720p などの基底画質
     * @param durationSeconds 番組尺 (秒)
     * @param isHEVC 通信節約モード (HEVC) が有効か
     * @returns 720p (約 2.2GB / 平均4.9Mbps) などの文字列
     */
    static formatQualitySelectLabel(baseQuality: string, durationSeconds: number, isHEVC: boolean): string {

        const apiQuality = isHEVC === true ? `${baseQuality}-hevc` : baseQuality;
        const qualityLabel = OfflineVideos.QUALITY_LABELS[baseQuality] ?? baseQuality;
        const sizeLabel = OfflineVideos.formatOfflineSize(
            OfflineVideos.estimateDisplaySizeBytes(durationSeconds, apiQuality),
            true,
        );
        const bitrateLabel = OfflineVideos.formatAverageBitrateLabel(apiQuality);
        return `${qualityLabel} (${sizeLabel} / 平均${bitrateLabel})`;
    }

    /**
     * 録画一覧メニュー向けに、既定画質 (720p) の見積もり容量を返す。
     * @param program 録画番組
     * @param isHEVC 通信節約モード (HEVC) を既定で使うか
     * @returns 約 2.2GB などの文字列
     */
    static formatDefaultMenuSizeLabel(program: IRecordedProgram, isHEVC: boolean): string {

        const apiQuality = isHEVC === true ? '720p-hevc' : '720p';
        return OfflineVideos.formatOfflineSize(
            OfflineVideos.estimateDisplaySizeBytes(program.recorded_video.duration, apiQuality),
            true,
        );
    }

    /**
     * API 画質文字列から server/app/constants.py 相当の平均ビットレートを取得する。
     * @param apiQuality API 画質
     * @returns 映像・音声ビットレート (kbps)
     */
    private static getStreamQualityBitrates(apiQuality: string): {
        video_bitrate_kbps: number;
        video_bitrate_max_kbps: number;
        audio_bitrate_kbps: number;
    } {

        // 10bit / 24fps は平均ビットレートへ影響しないため、ルックアップキーから除外する
        const lookupQuality = apiQuality.replace(/-10bit/g, '').replace(/-24fps/g, '');
        const bitrates = OfflineVideos.STREAM_QUALITY_BITRATES[lookupQuality];
        if (bitrates !== undefined) return bitrates;

        const baseQuality = lookupQuality.replace(/-hevc/g, '');
        return OfflineVideos.STREAM_QUALITY_BITRATES[baseQuality] ?? {
            video_bitrate_kbps: 0,
            video_bitrate_max_kbps: 0,
            audio_bitrate_kbps: 0,
        };
    }

    /**
     * VBR エンコードの実効平均ビットレートを返す。
     * VideoEncodingTask が -vb / --vbr / --qvbr で指定する video_bitrate は目標値のため、映像側だけ係数を掛けて実測に近づける。
     * @param apiQuality API 画質
     * @returns 実効映像・音声・合算ビットレート (kbps)
     */
    private static getEffectiveStreamBitrates(apiQuality: string): {
        video_bitrate_kbps: number;
        audio_bitrate_kbps: number;
        total_bitrate_kbps: number;
    } {

        const bitrates = OfflineVideos.getStreamQualityBitrates(apiQuality);
        const isHEVC = apiQuality.includes('-hevc') === true;
        const effectiveVideoRatio = isHEVC === true
            ? OfflineVideos.VBR_EFFECTIVE_VIDEO_BITRATE_RATIO.hevc
            : OfflineVideos.VBR_EFFECTIVE_VIDEO_BITRATE_RATIO.avc;
        const effectiveVideoBitrateKbps = bitrates.video_bitrate_kbps * effectiveVideoRatio;
        const totalBitrateKbps = effectiveVideoBitrateKbps + bitrates.audio_bitrate_kbps;
        return {
            video_bitrate_kbps: effectiveVideoBitrateKbps,
            audio_bitrate_kbps: bitrates.audio_bitrate_kbps,
            total_bitrate_kbps: totalBitrateKbps,
        };
    }

    /**
     * API 画質文字列から利用者向けの画質ラベルを返す。
     * HEVC / 10bit / 24fps などの内部接尾辞は一覧表示では省略する。
     * @param quality API 画質
     * @returns 720p や 1080p (60fps) などの表示ラベル
     */
    static formatQualityLabel(quality: string): string {

        // 保存開始時と同じ順序で付与される接尾辞だけを外し、解像度ベースのラベルへ戻す
        const baseQuality = quality.replace(/-hevc/g, '').replace(/-10bit/g, '').replace(/-24fps/g, '');
        return OfflineVideos.QUALITY_LABELS[baseQuality] ?? baseQuality;
    }

    /**
     * 終端状態の保存ジョブを一覧から消す。
     * @param jobID 保存ジョブ ID
     */
    static async dismissJob(jobID: string): Promise<void> {
        await OfflineVideoStorage.deleteJob(jobID);
    }

    /**
     * 保存ジョブをキャンセルする。
     * @param jobID 保存ジョブ ID
     */
    static async cancel(jobID: string): Promise<void> {

        // 完了確定と同じ IndexedDB 上でキャンセルを確定し、勝った処理だけが保存世代を削除する
        const job = await OfflineVideoStorage.transitionActiveJobToTerminalState(jobID, 'Cancelled', null);
        if (job === null) return;

        // 通信の準備中は Background Fetch のジョブもこのページが中止する。
        this.jobAbortControllers.get(jobID)?.abort();
        this.cancellationChannel?.postMessage(jobID);
        try {
            if (job.background_fetch_id !== null && 'serviceWorker' in navigator) {
                const registration = await navigator.serviceWorker.getRegistration();
                const backgroundFetch = await registration?.backgroundFetch?.get(job.background_fetch_id);
                await backgroundFetch?.abort();
            }
        } finally {
            await OfflineVideoStorage.deleteGeneration(job.video_id, job.generation_id);
        }
    }

    /**
     * 保存済み動画を端末から削除する。
     * @param videoID 録画番組 ID
     */
    static async deleteVideo(videoID: number): Promise<void> {

        const video = await this.getVideo(videoID);
        if (video === null) return;
        await OfflineVideoStorage.deleteVideo(video);
    }

    /**
     * 保存済み動画の HLS プレイリスト URLを返す。
     * @param video 保存済み動画
     * @returns Service Worker が配信するプレイリスト URL
     */
    static getPlaylistURL(video: IOfflineVideo): string {
        return OfflineVideoStorage.getPlaylistURL(video);
    }

    /**
     * 保存済み動画に付随する画像・JSON の URLを返す。
     * @param video 保存済み動画
     * @param assetName 付随データ名
     * @returns Service Worker が配信する付随データ URL
     */
    static getAssetURL(video: IOfflineVideo, assetName: 'thumbnail.webp' | 'thumbnail-tiled.webp' | 'channel-logo' | 'jikkyo.json'): string {
        return OfflineVideoStorage.getAssetURL(video, assetName);
    }

    private static getForegroundLockName(jobID: string): string {
        return `konomitv-offline-foreground-${jobID}`;
    }

    private static getVideoStartLockName(videoID: number): string {
        return `konomitv-offline-start-${videoID}`;
    }

    /**
     * 前景保存中のページ凍結を抑えるローカル WebRTC 接続を開始する。
     * @returns WebRTC 接続を閉じる関数（利用できない環境では null）
     */
    private static async acquireForegroundActivityProtection(signal: AbortSignal): Promise<(() => void) | null> {

        if ('RTCPeerConnection' in window === false) return null;
        signal.throwIfAborted();

        const offerPeerConnection = new RTCPeerConnection({iceServers: []});
        const answerPeerConnection = new RTCPeerConnection({iceServers: []});
        const offerDataChannel = offerPeerConnection.createDataChannel('konomitv-offline-download');
        const dataChannels = [offerDataChannel];
        const closeConnections = () => {
            dataChannels.forEach(dataChannel => dataChannel.close());
            offerPeerConnection.close();
            answerPeerConnection.close();
        };

        const {controller, dispose} = this.linkAbortController(signal);
        const preparationSignal = controller.signal;
        // Offer や Answer の生成も含めて、準備全体の待機時間を制限する。
        const timeoutID = setTimeout(() => {
            controller.abort(new Error('WebRTC のローカル接続準備がタイムアウトしました。'));
        }, 15_000);

        const waitUntilReady = async (target: EventTarget, event: string, isReady: () => boolean): Promise<void> => {
            preparationSignal.throwIfAborted();
            if (isReady()) return;
            await new Promise<void>(resolve => {
                const onReady = () => {
                    if (isReady()) {
                        target.removeEventListener(event, onReady);
                        resolve();
                    }
                };
                target.addEventListener(event, onReady, {signal: preparationSignal});
            });
        };

        const connect = async (): Promise<void> => {
            answerPeerConnection.addEventListener('datachannel', (event) => {
                dataChannels.push(event.channel);
            }, {once: true, signal: preparationSignal});

            // ICE 候補を含む Offer / Answer を同一ページ内で交換し、ネットワーク上の相手を必要としない接続にする
            await offerPeerConnection.setLocalDescription(await offerPeerConnection.createOffer());
            await waitUntilReady(offerPeerConnection, 'icegatheringstatechange', () => offerPeerConnection.iceGatheringState === 'complete');
            const offerDescription = offerPeerConnection.localDescription;
            if (offerDescription === null) {
                throw new Error('WebRTC の Offer を作成できませんでした。');
            }
            await answerPeerConnection.setRemoteDescription(offerDescription);
            await answerPeerConnection.setLocalDescription(await answerPeerConnection.createAnswer());
            await waitUntilReady(answerPeerConnection, 'icegatheringstatechange', () => answerPeerConnection.iceGatheringState === 'complete');
            const answerDescription = answerPeerConnection.localDescription;
            if (answerDescription === null) {
                throw new Error('WebRTC の Answer を作成できませんでした。');
            }
            await offerPeerConnection.setRemoteDescription(answerDescription);

            // Chrome が保護対象と判定するのは開いた DataChannel なので、接続完了後に動画転送へ進む
            await waitUntilReady(offerDataChannel, 'open', () => offerDataChannel.readyState === 'open');
        };

        try {
            preparationSignal.throwIfAborted();
            // close() 後に Offer / Answer の Promise が未解決でも、保存処理の待機を終了する。
            const interrupted = new Promise<never>((_, reject) => {
                preparationSignal.addEventListener('abort', () => reject(preparationSignal.reason), {once: true});
            });
            await Promise.race([connect(), interrupted]);
            preparationSignal.throwIfAborted();
            return closeConnections;
        } catch (error) {
            closeConnections();
            throw error;
        } finally {
            clearTimeout(timeoutID);
            dispose();
            controller.abort();
        }
    }

    /**
     * 録画番組 ID ごとに start() の本体処理を直列化する。
     * Web Locks 対応環境ではタブ間も含めて排他し、非対応環境では同一タブ内だけを直列化する。
     */
    private static async withVideoStartLock<T>(videoID: number, operation: () => Promise<T>): Promise<T> {

        if ('locks' in navigator) {
            return await navigator.locks.request(this.getVideoStartLockName(videoID), operation);
        }

        const previous = this.startMutexTailByVideoID.get(videoID) ?? Promise.resolve();
        let releaseCurrent!: () => void;
        const currentGate = new Promise<void>(resolve => {
            releaseCurrent = resolve;
        });
        const queued = previous.then(() => currentGate);
        this.startMutexTailByVideoID.set(videoID, queued);

        await previous;
        try {
            return await operation();
        } finally {
            releaseCurrent();
            if (this.startMutexTailByVideoID.get(videoID) === queued) {
                this.startMutexTailByVideoID.delete(videoID);
            }
        }
    }

    /** 前景保存中だけタブ閉じ警告を有効化する */
    private static updateForegroundDownloadWarning(): void {
        if (this.foregroundLockReleases.size > 0) {
            window.addEventListener('beforeunload', OfflineVideos.onBeforeUnload);
        } else {
            window.removeEventListener('beforeunload', OfflineVideos.onBeforeUnload);
        }
    }

    private static async acquireForegroundLock(jobID: string): Promise<(() => void) | null> {
        if ('locks' in navigator === false) return null;

        let resolveLockAcquired: (() => void) | null = null;
        let rejectLockAcquired: ((reason: unknown) => void) | null = null;
        let resolveLockRelease: (() => void) | null = null;
        let isLockAcquired = false;
        const lockAcquired = new Promise<void>((resolve, reject) => {
            resolveLockAcquired = resolve;
            rejectLockAcquired = reject;
        });
        const lockRelease = new Promise<void>((resolve) => {
            resolveLockRelease = resolve;
        });

        // リクエストを完了させず保持し、別タブが同じジョブを中断済みと誤認するのを防ぐ
        void navigator.locks.request(this.getForegroundLockName(jobID), async () => {
            this.foregroundLockReleases.set(jobID, () => resolveLockRelease?.());
            isLockAcquired = true;
            resolveLockAcquired?.();
            await lockRelease;
            this.foregroundLockReleases.delete(jobID);
            this.updateForegroundDownloadWarning();
        }).catch((error: unknown) => {
            // ロック取得前の API 失敗を呼び出し元へ返し、開始処理を永久待機させない
            if (isLockAcquired === false) rejectLockAcquired?.(error);
        });
        await lockAcquired;
        this.updateForegroundDownloadWarning();
        return () => resolveLockRelease?.();
    }

    /**
     * 保存状態の変更を同一コンテキストと、開いているページへ通知する。
     * Service Worker 内からは EventTarget がページと共有されないため、postMessage でも伝える。
     */
    private static notifyChange(): void {

        OfflineVideos.eventTarget.dispatchEvent(new Event('change'));

        // Service Worker 側の EventTarget はページから参照できないため、開いているクライアントへ直接通知する
        if (typeof window === 'undefined') {
            const serviceWorkerScope = self as unknown as ServiceWorkerGlobalScope;
            void serviceWorkerScope.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
                for (const client of clients) {
                    client.postMessage({ type: 'konomitv-offline-videos-change' });
                }
            });
        }
    }

}

// Service Worker からの保存状態更新をページ側 EventTarget へ橋渡しする
if (typeof window !== 'undefined' && 'serviceWorker' in navigator) {
    navigator.serviceWorker.addEventListener('message', (event: MessageEvent) => {
        if (event.data?.type === 'konomitv-offline-videos-change') {
            OfflineVideos.eventTarget.dispatchEvent(new Event('change'));
        }
    });
}
