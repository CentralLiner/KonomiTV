"""オフライン保存の再接続と終了を検証する。"""

import asyncio
import concurrent.futures
import errno
import io
import os
import struct
import sys
import threading
import uuid
from collections import Counter
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from biim.mpeg2ts import ts
from biim.mpeg2ts.parser import PESParser
from biim.mpeg2ts.pes import PES
from starlette.types import Message, Send

from app import schemas
from app.constants import JST, LIBRARY_PATH, PROCESS_POOL_CONTEXT, QUALITY_TYPES
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.streams.OfflineVideoSession import (
    OFFLINE_VIDEO_STREAM_SEMAPHORE,
    OfflineVideoSession,
)
from app.streams.VideoEncodingTask import VideoEncodingTask
from app.utils.TSKeyFrameSeeker import TSKeyFrameCollector
from tests.conftest import FIXTURES_DIR


@pytest.fixture(scope='module')
def analyzed_recordings() -> dict[Path, schemas.RecordedProgram]:
    """
    同じ録画フィクスチャの解析結果をテスト間で再利用する。

    Args:
        なし

    Returns:
        dict[Path, schemas.RecordedProgram]: 元のファイルパスごとの解析結果
    """
    return {}


@pytest.fixture(params=['terrestrial-bdav-partial-ts.m2ts'], ids=['M2TS'])
async def recording(
    client: httpx.AsyncClient,
    tmp_path: Path,
    request: pytest.FixtureRequest,
    analyzed_recordings: dict[Path, schemas.RecordedProgram],
) -> AsyncGenerator[tuple[int, Path]]:
    """
    本番の録画スキャンとは独立に、短い録画をテスト用 DB に登録する。

    Args:
        client (httpx.AsyncClient): サーバーの起動処理を済ませるテスト用 API クライアント
        tmp_path (Path): 変更可能な録画のコピーを置くフォルダ
        request (pytest.FixtureRequest): 録画フィクスチャ名を取得するためのリクエスト
        analyzed_recordings (dict[Path, schemas.RecordedProgram]): フィクスチャの解析結果

    Returns:
        AsyncGenerator[tuple[int, Path]]: 登録した録画の ID とパス
    """
    file_name: str = request.param
    source = FIXTURES_DIR / 'recorded' / file_name

    # 本番の録画スキャンと同様に、各フィクスチャを専用プロセスで一度だけ解析する。
    analyzed = analyzed_recordings.get(source)
    if analyzed is None:
        with concurrent.futures.ProcessPoolExecutor(max_workers=1, mp_context=PROCESS_POOL_CONTEXT) as executor:
            analyzed = await asyncio.get_running_loop().run_in_executor(executor, MetadataAnalyzer(source).analyze)
        assert analyzed is not None
        analyzed_recordings[source] = analyzed

    # ファイルを変更するケースでも元のフィクスチャと他のテストを保護するため、コピーを登録する。
    path = tmp_path / source.name
    path.write_bytes(source.read_bytes())
    if path.suffix == '.mp4':
        path.with_suffix('.psc').write_bytes(source.with_suffix('.psc').read_bytes())
    stat = path.stat()
    program_fields = analyzed.model_dump(exclude={'id', 'channel', 'recorded_video', 'series_id', 'series_broadcast_period_id'})
    program = await RecordedProgram.create(**program_fields)
    video_fields = analyzed.recorded_video.model_dump(exclude={'id', 'recorded_program_id'})
    video_fields.update(
        file_path = str(path),
        file_size = stat.st_size,
        file_created_at = datetime.fromtimestamp(stat.st_ctime, tz=JST),
        file_modified_at = datetime.fromtimestamp(stat.st_mtime, tz=JST),
    )
    await RecordedVideo.create(recorded_program=program, **video_fields)
    try:
        yield program.id, path
    finally:
        await OfflineVideoSession.closeAll()
        await program.delete()


def ReadSegments(body: bytes, *, first_sequence: int = 0) -> tuple[list[bytes], int]:
    """
    完全に受信した応答のセグメント番号と終端件数を確認する。

    Args:
        body (bytes): HTTP 応答の本文
        first_sequence (int): GET で要求した最初のセグメント番号

    Returns:
        tuple[list[bytes], int]: 受信したセグメントの本体と終端件数
    """
    assert body.startswith(b'KTVODLP\n')
    offset = 12 + struct.unpack_from('>I', body, 8)[0]
    segments: list[bytes] = []
    while True:
        sequence, value = struct.unpack_from('>II', body, offset)
        offset += 8
        if sequence == 0xffffffff:
            assert offset == len(body)
            assert value == first_sequence + len(segments)
            return segments, value
        if sequence == 0xfffffffe:
            assert value == 0
            continue
        assert sequence == first_sequence + len(segments)
        assert value > 0
        length = struct.unpack_from('>I', body, offset)[0]
        offset += 4
        assert 0 < length <= OfflineVideoSession.MAX_SEGMENT_BYTES
        segments.append(body[offset:offset + length])
        offset += length


@pytest.mark.parametrize('recording', [
    'terrestrial-bdav-partial-ts.m2ts',
    'terrestrial-tsreplace-h264.ts',
    'terrestrial-psc-sidecar.mp4',
], indirect=True)
async def test_offline_video_session_replays_the_same_output(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
    """
    セッション作成と再接続を繰り返し、確定位置から同じデータを受信できることを確認する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス

    Returns:
        None
    """
    video_id, _ = recording
    # 実出力が仮想プレイリストの終端を越える場合も検証する。
    await RecordedVideo.filter(recorded_program_id=video_id).update(duration=1.0)
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created, repeated = await asyncio.gather(client.put(url), client.put(url))
    assert sorted([created.status_code, repeated.status_code]) == [200, 201]
    assert created.json() == repeated.json()
    assert created.json()['protocol_version'] == 2
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    assert (await client.get(f'{url}?connection_id=1&next_sequence=0')).status_code == 409

    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count >= 2
    assert all(segment.startswith(b'\x47') for segment in segments)
    recorded_video = await RecordedVideo.get(recorded_program_id=video_id)
    assert all(entry['sequence_index'] == 0 for entry in recorded_video.segment_map)

    replay = await client.get(f'{url}?connection_id=2&next_sequence=1', headers=headers)
    assert replay.status_code == 200
    assert ReadSegments(replay.content, first_sequence=1) == (segments[1:], count)
    stale_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 1, 'next_sequence': count}, headers=headers)
    assert stale_ack.status_code == 409
    future_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 2, 'next_sequence': count + 1}, headers=headers)
    assert future_ack.status_code == 409
    smaller_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 2, 'next_sequence': 0}, headers=headers)
    assert smaller_ack.json()['next_sequence'] == 1
    assert (await client.get(f'{url}?connection_id=1&next_sequence=1', headers=headers)).status_code == 409
    assert (await client.get(f'{url}?connection_id=3&next_sequence=0', headers=headers)).status_code == 410

    end = await client.get(f'{url}?connection_id=4&next_sequence={count}', headers=headers)
    assert ReadSegments(end.content, first_sequence=count) == ([], count)
    assert (await client.delete(url, headers=headers)).status_code == 204
    assert (await client.delete(url, headers=headers)).status_code == 204
    assert (await client.get(f'{url}?connection_id=5&next_sequence={count}', headers=headers)).status_code == 410
    replacement = await client.put(url)
    assert replacement.status_code == 201
    assert replacement.json()['generation_token'] != created.json()['generation_token']
    assert (await client.delete(url, headers=headers)).status_code == 409


async def test_offline_video_session_revalidates_recording_on_reconnect(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    同じ録画への再接続は一時的な I/O 障害から復帰し、保存開始後の実ファイルの変更は拒否する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): ファイル照合の一時的な失敗の再現に使用

    Returns:
        None
    """
    video_id, path = recording
    # ゼロ埋めされた録画では、末尾の余白を除いた範囲が登録済みのハッシュと一致する。
    with path.open('r+b') as file:
        padded_size = path.stat().st_size + 1024 * 1024
        file.truncate(padded_size)
    await RecordedVideo.filter(recorded_program_id=video_id).update(file_size=padded_size)
    # DB に登録済みの録画と同じ内容なら、保存開始前の更新日時の差を許容する。
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    original_stat = Path.stat
    # ファイルサイズの照合と全体のハッシュ計算の後、ゼロ埋め境界の探索で I/O が失敗する。
    calls_until_failure = 3

    def StatWithTemporaryFailure(target: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        nonlocal calls_until_failure
        if target == path and calls_until_failure > 0:
            calls_until_failure -= 1
            if calls_until_failure == 0:
                raise OSError(errno.EIO, 'Temporary recording I/O error')
        return original_stat(target, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, 'stat', StatWithTemporaryFailure)
    interrupted_hash_check = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert interrupted_hash_check.status_code == 503
    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count > 1

    # 再接続時の照合で一度失敗しても、次の GET で既存のデータを取り直せる。
    calls_until_failure = 1
    retry = await client.get(f'{url}?connection_id=2&next_sequence=1', headers=headers)
    assert retry.status_code == 503
    replay = await client.get(f'{url}?connection_id=3&next_sequence=1', headers=headers)
    assert ReadSegments(replay.content, first_sequence=1) == (segments[1:], count)

    # GET の照合は通し、正常終端を公開する直前の照合で失敗させる。
    calls_until_failure = 2
    interrupted = await client.get(f'{url}?connection_id=4&next_sequence={count}', headers=headers)
    assert interrupted.status_code == 200
    with pytest.raises(struct.error):
        ReadSegments(interrupted.content, first_sequence=count)
    end = await client.get(f'{url}?connection_id=5&next_sequence={count}', headers=headers)
    assert ReadSegments(end.content, first_sequence=count) == ([], count)

    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000_000))
    changed = await client.get(f'{url}?connection_id=6&next_sequence={count}', headers=headers)
    assert changed.status_code == 409
    assert changed.json()['detail']['code'] == 'recording_changed'


async def test_offline_video_session_cancels_gets_waiting_for_recording_check(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    同時の GET は照合を共有し、DELETE 後も停止した I/O は復帰まで枠を占有する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 録画のハッシュ照合を一時停止するために使用

    Returns:
        None
    """
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}

    loop = asyncio.get_running_loop()
    started: asyncio.Queue[None] = asyncio.Queue()
    release = threading.Event()
    original_matches = MetadataAnalyzer.matchesFileHash

    def MatchWithStalledRead(analyzer: MetadataAnalyzer, file_hash: str, *, raise_on_error: bool = False) -> bool:
        # NAS が一時的に応答しない状況を、録画ハッシュの読み取りで再現する。
        matches = original_matches(analyzer, file_hash, raise_on_error=raise_on_error)
        loop.call_soon_threadsafe(started.put_nowait, None)
        assert release.wait(10)
        return matches

    monkeypatch.setattr(MetadataAnalyzer, 'matchesFileHash', MatchWithStalledRead)
    monkeypatch.setattr(OfflineVideoSession, '_file_io_slots', asyncio.Semaphore(1))
    requests = [
        asyncio.create_task(client.get(f'{url}?connection_id={connection_id}&next_sequence=0', headers=headers))
        for connection_id in (1, 2)
    ]
    next_url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    next_headers = {}
    try:
        await asyncio.wait_for(started.get(), timeout=5)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(started.get(), timeout=0.2)
        deleted = await asyncio.wait_for(client.delete(url, headers=headers), timeout=2)
        assert deleted.status_code == 204
        done, pending = await asyncio.wait(requests, timeout=2)
        assert not pending
        assert all(response.result().status_code == 410 for response in done)

        # RAM セッションは新しく作れても、停止した読み取りが復帰するまで次の照合は待機する。
        next_created = await client.put(next_url)
        assert next_created.status_code == 201
        next_headers = {'X-KonomiTV-Offline-Generation': next_created.json()['generation_token']}
        requests.append(asyncio.create_task(client.get(f'{next_url}?connection_id=1&next_sequence=0', headers=next_headers)))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(started.get(), timeout=0.2)
        release.set()
        await asyncio.wait_for(started.get(), timeout=5)
    finally:
        release.set()
        if next_headers:
            await asyncio.wait_for(client.delete(next_url, headers=next_headers), timeout=5)
        await asyncio.gather(*requests, return_exceptions=True)


@pytest.mark.parametrize(('quality', 'frame_rate', 'frame_count', 'wrap_dts'), [
    ('1080p-60fps', 60000, 361, False),
    ('720p', 30000, 181, True),
    ('540p-24fps', 24000, 145, False),
])
async def test_offline_video_session_keeps_final_boundary_frame_with_audio(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    quality: str,
    frame_rate: int,
    frame_count: int,
    wrap_dts: bool,
) -> None:
    """
    分割境界にある最終フレームを、主音声・副音声と同じ末尾セグメントに保持する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 末尾を分割境界にそろえるために使用
        quality (str): 保存に指定する画質
        frame_rate (int): 映像のフレームレートを表す分子
        frame_count (int): エンコーダーから出力するフレーム数
        wrap_dts (bool): DTS の折り返しを発生させるか

    Returns:
        None
    """
    original_build = VideoEncodingTask.buildFFmpegOptions

    def BuildWithFinalKeyFrame(task: VideoEncodingTask, quality: QUALITY_TYPES, output_ts_offset: float) -> list[str]:
        if wrap_dts:
            output_ts_offset = (ts.PCR_CYCLE // ts.HZ) - 4
        options = original_build(task, quality, output_ts_offset)
        # 実録画を最後まで読み、6.006 秒の分割境界に最後のキーフレームを配置する。
        options[options.index('-vf') + 1] += f',fps={frame_rate}/1001,trim=end_frame={frame_count},setpts=PTS-STARTPTS'
        options[options.index('-af') + 1] += f',asetpts=PTS-STARTPTS,atrim=duration={frame_count * 1001 / frame_rate}'
        if '-r' in options:
            rate_index = options.index('-r')
            del options[rate_index:rate_index + 2]
        options[-1:-1] = ['-sc_threshold', '0', '-force_key_frames', f'expr:eq(n,{frame_count - 1})', '-fps_mode', 'passthrough']
        if wrap_dts:
            # B フレームを加え、PTS と別に記録される DTS が 33bit の境界をまたぐ出力を生成する。
            options[-1:-1] = ['-bf', '2']
        return options

    monkeypatch.setattr(VideoEncodingTask, 'buildFFmpegOptions', BuildWithFinalKeyFrame)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/{quality}/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}

    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count == 1

    # FFmpeg の既定 PID ごとに数え、最終フレームと両音声が同じセグメントに含まれることを確かめる。
    packet_counts: Counter[int] = Counter()
    video_frames = 0
    video_dts: list[int] = []
    for offset in range(0, len(segments[0]), ts.PACKET_SIZE):
        packet = segments[0][offset:offset + ts.PACKET_SIZE]
        pid = ts.pid(packet)
        packet_counts[pid] += 1
        if pid == 0x100 and ts.payload_unit_start_indicator(packet):
            video_frames += 1
            if wrap_dts:
                timestamp = PES(ts.payload(packet)).dts()
                if timestamp is not None:
                    video_dts.append(timestamp)
    assert video_frames == frame_count
    assert packet_counts[0x101] > 0
    assert packet_counts[0x102] > 0
    if wrap_dts:
        assert video_dts[0] > ts.PCR_CYCLE - 10 * ts.HZ
        assert video_dts[-1] < 10 * ts.HZ
        metadata_length = struct.unpack_from('>I', response.content, 8)[0]
        duration_ms = struct.unpack_from('>I', response.content, 16 + metadata_length)[0]
        assert 6000 <= duration_ms <= 6100


async def test_offline_video_session_keeps_waiting_encoder_on_disconnect(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    実行枠を待つ間に切断された応答や送信中の旧応答から、新しい接続へ引き継げることを確認する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): keepalive の間隔を短縮するために使用

    Returns:
        None
    """
    from app.app import app

    monkeypatch.setattr(OfflineVideoSession, 'KEEPALIVE_INTERVAL', 0.05)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    # ASGI send 自体が失敗する場合を再現する。次の要求は旧応答が送信中の間に接続する。
    half_open = asyncio.Event()
    replaced = asyncio.Event()
    hold = asyncio.Event()

    async def RunConnection(connection_id: int, break_at_headers: bool = False) -> None:
        async def Receive() -> Message:
            await hold.wait()
            return {'type': 'http.disconnect'}

        async def Send(message: Message) -> None:
            if message['type'] == 'http.response.start' and break_at_headers:
                raise OSError('Injected disconnect during response headers')
            if message.get('body') == struct.pack('>II', 0xfffffffe, 0):
                half_open.set()
                await hold.wait()
            if message['type'] == 'http.response.body' and message.get('more_body') is False:
                replaced.set()

        await app({
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.4'},
            'http_version': '1.1',
            'scheme': 'http',
            'method': 'GET',
            'path': url,
            'raw_path': url.encode('ascii'),
            'query_string': f'connection_id={connection_id}&next_sequence=0'.encode('ascii'),
            'headers': [(key.lower().encode('ascii'), value.encode('ascii')) for key, value in headers.items()],
        }, Receive, Send)

    async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
        await RunConnection(1, break_at_headers=True)
        old_response = asyncio.create_task(RunConnection(2))
        try:
            await asyncio.wait_for(half_open.wait(), timeout=5)
            ack = await client.put(f'{url}/checkpoint', headers=headers, json={'connection_id': 2, 'next_sequence': 0})
            assert ack.status_code == 200
            new_response = asyncio.create_task(client.get(f'{url}?connection_id=3&next_sequence=0', headers=headers))
            await asyncio.wait_for(replaced.wait(), timeout=5)
            await asyncio.wait_for(old_response, timeout=5)
        finally:
            hold.set()
            await old_response
    response = await asyncio.wait_for(new_response, timeout=30)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count >= 2


async def test_offline_video_session_reconnects_while_waiting_for_capacity(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    未確認データと送信中の参照を保持し、再接続後の ACK で同じエンコーダーを再開する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): RAM の上限とエンコーダーの待機を観察するために使用

    Returns:
        None
    """
    from app.app import app

    # 次のセグメントの予約が、前のセグメントの解放を待つ大きさにする。
    monkeypatch.setattr(OfflineVideoSession, 'MAX_SEGMENT_BYTES', 1024 * 1024)
    monkeypatch.setattr(OfflineVideoSession, 'RETAINED_BYTES_LIMIT', 1024 * 1024)
    produced: list[bytes] = []
    encoders: set[asyncio.subprocess.Process] = set()
    capacity_waiting = asyncio.Event()
    capacity_resumed = asyncio.Event()
    original_publish = OfflineVideoSession.publishSegment
    original_wait = OfflineVideoSession.waitForCapacity

    def PublishSegment(session: OfflineVideoSession, sequence: int, data: bytearray, duration: float) -> None:
        original_publish(session, sequence, data, duration)
        produced.append(bytes(data))

    async def WaitForCapacity(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        encoders.add(encoder)
        if len(produced) == 1:
            capacity_waiting.set()
        available = await original_wait(session, encoder)
        if len(produced) == 1 and available:
            capacity_resumed.set()
        return available

    monkeypatch.setattr(OfflineVideoSession, 'publishSegment', PublishSegment)
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', WaitForCapacity)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    replay = bytearray()

    async def RunConnection(connection_id: int) -> None:
        sequence: int | None = None
        length = 0

        async def Receive() -> Message:
            await asyncio.Event().wait()
            return {'type': 'http.disconnect'}

        async def Send(message: Message) -> None:
            nonlocal sequence, length
            if message['type'] == 'http.response.start':
                assert message['status'] == 200
                return
            body = message.get('body', b'')
            if connection_id == 2:
                replay.extend(body)
            if sequence is not None:
                assert len(body) == length
                assert body == produced[sequence]
                if connection_id == 1:
                    # 端末へ確定できなかった本文を、同じ bytes のまま再送させる。
                    await capacity_waiting.wait()
                    raise OSError('Injected disconnect during segment body')
                ack = await client.put(
                    f'{url}/checkpoint',
                    headers = headers,
                    json = {'connection_id': connection_id, 'next_sequence': sequence + 1},
                )
                assert ack.status_code == 200
                if sequence == 0:
                    # ACK が来ても、この send が本体を参照する間は次の生成を待たせる。
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(capacity_resumed.wait(), timeout=0.05)
                    assert len(produced) == 1
                sequence = None
            elif len(body) == 12:
                sequence, _, length = struct.unpack('>III', body)

        await app({
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.4'},
            'http_version': '1.1',
            'scheme': 'http',
            'method': 'GET',
            'path': url,
            'raw_path': url.encode('ascii'),
            'query_string': f'connection_id={connection_id}&next_sequence=0'.encode('ascii'),
            'headers': [(key.lower().encode('ascii'), value.encode('ascii')) for key, value in headers.items()],
        }, Receive, Send)

    async with asyncio.timeout(30):
        await RunConnection(1)
        assert capacity_waiting.is_set()
        assert not capacity_resumed.is_set()
        assert len(produced) == 1
        await RunConnection(2)
    segments, count = ReadSegments(bytes(replay))
    assert segments == produced
    assert len(segments) == count >= 2
    assert capacity_resumed.is_set()
    assert len(encoders) == 1
    assert (await client.delete(url, headers=headers)).status_code == 204


async def test_offline_video_session_cancels_pipe_registration(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    パイプの登録途中で DELETE すると、登録の完了後にパイプと実行枠を解放する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): パイプの登録が完了する直前に待機させるために使用

    Returns:
        None
    """
    waiting = asyncio.Event()
    release = asyncio.Event()
    original_to_thread = anyio.to_thread.run_sync
    original_pipe = os.pipe
    pipe_fds: list[int] = []

    def TrackPipe() -> tuple[int, int]:
        pipe = original_pipe()
        pipe_fds.extend(pipe)
        return pipe

    async def DelayPipeRegistration[T](func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        result = await original_to_thread(func, *args, **kwargs)
        # 書き込み側 FD の登録が済み、呼び出し元へ結果を返す直前で待つ。
        if len(args) == 1 and args[0] in pipe_fds[1::2]:
            waiting.set()
            await release.wait()
        return result

    monkeypatch.setattr(os, 'pipe', TrackPipe)
    monkeypatch.setattr(anyio.to_thread, 'run_sync', DelayPipeRegistration)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    try:
        await asyncio.wait_for(waiting.wait(), timeout=10)
        delete_task = asyncio.create_task(client.delete(url, headers=headers))
        # 終了処理が登録の完了を待ち、その後でパイプの両端を閉じることを確認する。
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(delete_task), timeout=0.05)
        release.set()
        deleted = await asyncio.wait_for(delete_task, timeout=3)
        assert deleted.status_code == 204
        for pipe_fd in pipe_fds:
            with pytest.raises(OSError):
                os.fstat(pipe_fd)
        assert (await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)).status_code == 410
        async with asyncio.timeout(1):
            async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
                pass
    finally:
        # 修正前のコードで失敗した場合も、待機を解除してテスト用の資源を回収する。
        release.set()
        await client.delete(url, headers=headers)
        await response_task


async def test_offline_video_session_closes_input_pipe_on_cancel_during_cleanup(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    エンコード失敗後の回収中に DELETE が届いても、未起動のフィードに渡した書き込み側 FD を閉じる。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 入力フィードの待機とエンコード失敗の再現に使用

    Returns:
        None
    """
    closing_input = asyncio.Event()
    allow_close = asyncio.Event()
    pipe_fd: int | None = None
    original_file_io = OfflineVideoSession.runFileIO
    original_to_thread = anyio.to_thread.run_sync

    async def TrackInputPipe[T](func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        nonlocal pipe_fd
        if len(args) == 1 and type(args[0]) is int:
            pipe_fd = args[0]
        if func.__name__ == 'CloseOfflineInput':
            closing_input.set()
            await allow_close.wait()
        return await original_to_thread(func, *args, **kwargs)

    async def QueueFeed[T](func: Callable[..., T], *args: Any) -> T:
        # 入力ワーカーが使用中で、フィードがまだキューにある状態を作る。
        if pipe_fd is not None and args and args[0] == pipe_fd:
            await asyncio.Future()
        return await original_file_io(func, *args)

    async def FailEncoding(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        raise RuntimeError('Encoder failed before the input worker started.')

    monkeypatch.setattr(anyio.to_thread, 'run_sync', TrackInputPipe)
    monkeypatch.setattr(OfflineVideoSession, 'runFileIO', staticmethod(QueueFeed))
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', FailEncoding)

    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    delete_task: asyncio.Task[httpx.Response] | None = None
    try:
        await asyncio.wait_for(closing_input.wait(), timeout=10)
        assert pipe_fd is not None
        delete_task = asyncio.create_task(client.delete(url, headers=headers))
        # DELETE が GET を終了させてから、キャンセルされたエンコードのパイプ回収を再開する。
        await asyncio.wait_for(response_task, timeout=5)
        allow_close.set()
        assert (await asyncio.wait_for(delete_task, timeout=5)).status_code == 204
        with pytest.raises(OSError):
            os.fstat(pipe_fd)
    finally:
        allow_close.set()
        if delete_task is not None:
            await delete_task
        await client.delete(url, headers=headers)
        await response_task
        # 修正前のコードで漏れを検出した場合も、テスト側で残った FD を閉じる。
        if pipe_fd is not None:
            try:
                os.close(pipe_fd)
            except OSError:
                pass


async def test_offline_video_session_sends_keepalive_during_final_validation(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    終端照合の I/O 待ちにも keepalive を送り、実装上の例外が起きた場合は保存を終了する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 終端照合の待ち時間と例外を再現する

    Returns:
        None
    """
    checking = asyncio.Event()
    release = asyncio.Event()
    keepalive = asyncio.Event()
    original_stream = OfflineVideoSession.stream
    fail_check = False

    async def CheckWithDelay(_session: OfflineVideoSession) -> bool:
        checking.set()
        await asyncio.wait_for(release.wait(), 10)
        if fail_check:
            raise RuntimeError('Unexpected recording validation error')
        return True

    async def TrackStream(session: OfflineVideoSession, connection_id: int, next_sequence: int, send: Send) -> None:
        async def TrackSend(message: Message) -> None:
            if checking.is_set() and message.get('body') == struct.pack('>II', 0xfffffffe, 0):
                keepalive.set()
            await send(message)

        # GET の照合が済んだ時点で、正常終端の公開前の照合に待ち時間を入れる。
        with monkeypatch.context() as patch:
            patch.setattr(OfflineVideoSession, '_matchesRecording', CheckWithDelay)
            await original_stream(session, connection_id, next_sequence, TrackSend)

    monkeypatch.setattr(OfflineVideoSession, 'stream', TrackStream)
    monkeypatch.setattr(OfflineVideoSession, 'KEEPALIVE_INTERVAL', 0.02)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    try:
        await asyncio.wait_for(checking.wait(), 10)
        await asyncio.wait_for(keepalive.wait(), 1)
        assert not response_task.done()
        release.set()
        response = await asyncio.wait_for(response_task, 5)
        assert response.status_code == 200
        segments, count = ReadSegments(response.content)
        assert len(segments) == count > 0

        # 再接続時の照合は通し、終端を公開する直前の照合で実装上の例外を発生させる。
        fail_check = True
        response = await client.get(f'{url}?connection_id=2&next_sequence={count}', headers=headers)
        assert response.status_code == 200
        with pytest.raises(struct.error):
            ReadSegments(response.content, first_sequence=count)
        response = await client.get(f'{url}?connection_id=3&next_sequence={count}', headers=headers)
        assert response.status_code == 410
    finally:
        release.set()
        await client.delete(url, headers=headers)
        await response_task


async def test_offline_video_session_legacy_stream(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
    """
    通常の録画再生でシークでき、Background Fetch 用の単一応答で正常終端まで取得できることを確認する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス

    Returns:
        None
    """
    video_id, _ = recording
    base_url = f'/api/streams/video/{video_id}/240p'
    # プレイリスト取得前のセグメント要求でセッションが作られ、そこから先頭へシークできることを確認する。
    params = {'session_id': uuid.uuid4().hex, 'cache_key': 'test'}
    later_segment = await client.get(f'{base_url}/segment', params={**params, 'sequence': 1})
    first_segment = await client.get(f'{base_url}/segment', params={**params, 'sequence': 0})
    for segment in (later_segment, first_segment):
        assert segment.status_code == 200
        assert segment.content.startswith(b'\x47') and len(segment.content) % ts.PACKET_SIZE == 0
    assert later_segment.content != first_segment.content

    # Background Fetch が使う旧形式は、単一リクエストで終端まで取得できる。
    await RecordedVideo.filter(recorded_program_id=video_id).update(duration=1.0)
    response = await client.get(f'{base_url}/offline-stream')
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count == 1

@pytest.mark.parametrize(('phase', 'recording'), [
    ('Feed', 'terrestrial-bdav-partial-ts.m2ts'),
    ('Retry', 'terrestrial-bdav-partial-ts.m2ts'),
    ('Feed', 'terrestrial-psc-sidecar.mp4'),
], indirect=['recording'])
async def test_offline_video_session_releases_slot_during_input_read(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """
    入力が停止していても、キャンセルや起動失敗から実行枠を回収する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 入力ファイルの読み取りを一時停止するために使用
        phase (str): 入力フィードの停止中に発生させる事象

    Returns:
        None
    """
    video_id, path = recording
    loop = asyncio.get_running_loop()
    read_started = asyncio.Event()
    file_closed = asyncio.Event()
    encoder_started = asyncio.Event()
    release_read = threading.Event()
    encoders: list[asyncio.subprocess.Process] = []
    original_wait = OfflineVideoSession.waitForCapacity
    input_open_count = 0

    class StalledFile(io.FileIO):
        did_block = False

        def readinto(self, buffer: Any) -> int | None:
            # BufferedReader が内部ロックを保持したまま、下層の read() を待つ状況を作る。
            if not self.did_block:
                self.did_block = True
                loop.call_soon_threadsafe(read_started.set)
                if not release_read.wait(15):
                    raise TimeoutError('Input read was not released by the test.')
            return super().readinto(buffer)

        def close(self) -> None:
            was_open = not self.closed
            super().close()
            if was_open and self.did_block:
                loop.call_soon_threadsafe(file_closed.set)

    def OpenInput(file_name: str, mode: str) -> io.BufferedReader:
        nonlocal input_open_count
        assert Path(file_name) == path and mode == 'rb'
        input_open_count += 1
        # TS は検索範囲の探索と入力の供給で別々にファイルを開く。
        if input_open_count == 2:
            return io.BufferedReader(StalledFile(file_name, mode))
        return open(file_name, mode)

    async def TrackEncoder(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        encoders.append(encoder)
        encoder_started.set()
        return await original_wait(session, encoder)

    encoding_module = sys.modules[VideoEncodingTask.__module__]
    if path.suffix == '.mp4':
        # エンコードの入力パイプだけを、停止する BufferedReader に差し替える。
        def OpenMP4Input(fd: int, mode: str) -> io.BufferedReader:
            return io.BufferedReader(StalledFile(fd, mode))

        monkeypatch.setattr(encoding_module, 'os', SimpleNamespace(**(vars(os) | {'fdopen': OpenMP4Input})))
    else:
        monkeypatch.setattr(encoding_module, 'open', OpenInput, raising=False)
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', TrackEncoder)
    # 各プールを1件に絞り、RAM 保存の read() が停止しても回収と通常の I/O が進むことを確認する。
    monkeypatch.setattr(OfflineVideoSession, '_file_io_slots', asyncio.Semaphore(1))
    default_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    original_run = loop.run_in_executor

    def RunWithLimitedInputPool(executor: concurrent.futures.Executor | None, func: Callable[..., Any], *args: Any) -> asyncio.Future[Any]:
        return original_run(default_executor if executor is None else executor, func, *args)

    monkeypatch.setattr(loop, 'run_in_executor', RunWithLimitedInputPool)
    session_id = uuid.uuid4().hex
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{session_id}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    get_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    delete_task: asyncio.Task[httpx.Response] | None = None
    try:
        await asyncio.wait_for(read_started.wait(), 5)
        assert (await asyncio.wait_for(asyncio.to_thread(path.stat), timeout=1)).st_size > 0
        await asyncio.wait_for(encoder_started.wait(), 5)
        if phase == 'Retry':
            # 入力のフィードが止まっている間に、映像・音声を出力する前のエンコーダーを終了させる。
            encoders[0].kill()
            response = await asyncio.wait_for(asyncio.shield(get_task), 8)
            assert response.status_code == 200
            with pytest.raises(struct.error):
                ReadSegments(response.content)
            assert len(encoders) == 1
            assert (await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)).status_code == 410
        else:
            delete_task = asyncio.create_task(client.delete(url, headers=headers))
            assert (await asyncio.wait_for(asyncio.shield(delete_task), 5)).status_code == 204
            assert OfflineVideoSession.get(session_id, video_id, '240p', created.json()['generation_token']) is None
        assert all(encoder.returncode is not None for encoder in encoders)
        # 読み取りの停止を解除する前に、Background Fetch と共有する3枠すべてを取得できる。
        async with asyncio.timeout(1):
            async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
                pass
        assert not file_closed.is_set()
    finally:
        release_read.set()
        if delete_task is not None:
            await asyncio.wait_for(delete_task, 10)
        else:
            await client.delete(url, headers=headers)
        await asyncio.wait_for(get_task, 10)
        if read_started.is_set():
            await asyncio.wait_for(file_closed.wait(), 5)
        monkeypatch.setattr(loop, 'run_in_executor', original_run)
        default_executor.shutdown(wait=True)


async def test_offline_video_session_capacity_and_recording_change(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    登録数の上限と期限切れによる回収、初回の接続前に変更された録画の拒否を確認する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): セッションの有効期限を短縮するために使用

    Returns:
        None
    """
    video_id, path = recording
    base_url = f'/api/streams/video/{video_id}/720p/offline-sessions'
    urls = [f'{base_url}/{uuid.uuid4().hex}' for _ in range(4)]
    for url in urls[:3]:
        assert (await client.put(url)).status_code == 201
    limit = await client.put(urls[3])
    assert limit.status_code == 429
    assert limit.headers['Retry-After'] == '5'

    session = await client.put(urls[0])
    headers = {'X-KonomiTV-Offline-Generation': session.json()['generation_token']}
    await client.delete(urls[0], headers=headers)
    monkeypatch.setattr(OfflineVideoSession, 'LEASE_TIMEOUT', 1)
    session = await client.put(urls[3])
    assert session.status_code == 201
    await asyncio.sleep(1.05)
    headers = {'X-KonomiTV-Offline-Generation': session.json()['generation_token']}
    assert (await client.get(f'{urls[3]}?connection_id=1&next_sequence=0', headers=headers)).status_code == 410

    session = await client.put(urls[3])
    assert session.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': session.json()['generation_token']}
    with path.open('ab') as file:
        file.write(b'changed')
    error = await client.get(f'{urls[3]}?connection_id=1&next_sequence=0', headers=headers)
    assert error.status_code == 409
    assert error.json()['detail']['code'] == 'recording_changed'


@pytest.mark.parametrize('failure', [
    'InitialProbe',
    'RetryLimit',
    'AfterPublication',
    'InputProgrammingError',
    'InputBrokenPipe',
])
async def test_offline_video_session_retries_probe_only_before_publication(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """
    公開前の解析失敗を再試行し、入力処理の例外と公開後の失敗ではセッションを終了する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): エンコーダーや入力処理の例外を再現するために使用
        failure (str): 再現する失敗の種類

    Returns:
        None
    """
    probe_durations: list[int] = []
    published_sequences: list[int] = []
    input_failures = 0
    original_build = VideoEncodingTask.buildFFmpegOptions
    original_publish = OfflineVideoSession.publishSegment
    original_wait = OfflineVideoSession.waitForCapacity

    def BuildWithProbeFailure(task: VideoEncodingTask, quality: QUALITY_TYPES, output_ts_offset: float) -> list[str]:
        options = original_build(task, quality, output_ts_offset)
        probe_durations.append(int(options[options.index('-analyzeduration') + 1]))
        if failure == 'AfterPublication':
            # 映像のみを実際にエンコードし、公開済みでも音声 PID が得られない場合を再現する。
            for stream in ('0:a:0', '0:a:1'):
                index = options.index(stream)
                del options[index - 1:index + 1]
            # 短い録画でも終端前に分割できるよう、キーフレーム間隔を短くする。
            options[options.index('-g') + 1] = '15'
        elif failure == 'RetryLimit' or (failure == 'InitialProbe' and len(probe_durations) == 1):
            # 解析時に音声を検出できなかった場合と同じ、FFmpeg の起動エラーを発生させる。
            options[options.index('0:a:1')] = '0:a:99'
        return options

    def TrackPublication(session: OfflineVideoSession, sequence: int, data: bytearray, duration_seconds: float) -> None:
        original_publish(session, sequence, data, duration_seconds)
        published_sequences.append(sequence)

    async def WaitAfterEarlyExit(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        if failure == 'RetryLimit' or (failure == 'InitialProbe' and len(probe_durations) == 1):
            # 容量判定より前に、実際の FFmpeg が音声の検出に失敗して終了した状態を作る。
            assert await asyncio.wait_for(encoder.wait(), timeout=5) != 0
        return await original_wait(session, encoder)

    monkeypatch.setattr(VideoEncodingTask, 'MAX_RETRY_COUNT', 2)
    monkeypatch.setattr(VideoEncodingTask, 'buildFFmpegOptions', BuildWithProbeFailure)
    monkeypatch.setattr(OfflineVideoSession, 'publishSegment', TrackPublication)
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', WaitAfterEarlyExit)
    if failure in ('InputProgrammingError', 'InputBrokenPipe'):
        def FailInputCollection(_collector: TSKeyFrameCollector, _chunk: bytes, _chunk_file_offset: int) -> None:
            nonlocal input_failures
            input_failures += 1
            if failure == 'InputBrokenPipe':
                raise BrokenPipeError(errno.EPIPE, 'Recording input failed')
            raise TypeError('Unexpected error in input keyframe collection')

        # 実録画の入力を最初のチャンクで止め、FFmpeg が映像・音声を出力する前の入力処理の例外を再現する。
        monkeypatch.setattr(TSKeyFrameCollector, 'push', FailInputCollection)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}

    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    assert len(probe_durations) == (2 if failure in ('InitialProbe', 'RetryLimit') else 1)
    if failure in ('InputProgrammingError', 'InputBrokenPipe'):
        assert input_failures == 1
    if len(probe_durations) == 2:
        assert probe_durations[1] > probe_durations[0]
    if failure == 'InitialProbe':
        segments, count = ReadSegments(response.content)
        assert len(segments) == count >= 2
        assert published_sequences == list(range(count))
        replay = await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)
        assert ReadSegments(replay.content) == (segments, count)
    else:
        assert bool(published_sequences) == (failure == 'AfterPublication')
        with pytest.raises(struct.error):
            ReadSegments(response.content)
        assert (await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)).status_code == 410
    async with asyncio.timeout(1):
        async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
            pass


async def test_offline_video_session_cancels_encoder_retry(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    起動失敗後のプロセス終了待ちで DELETE すると、その試行を回収して終了する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): tsreadex の終了待ちを再現するために使用

    Returns:
        None
    """
    waiting = asyncio.Event()
    encoders: list[asyncio.subprocess.Process] = []
    start_process = asyncio.subprocess.create_subprocess_exec

    async def StartWithStalledTSReadEx(program: str, *args: str, **kwargs: Any) -> asyncio.subprocess.Process:
        """
        tsreadex の終了待ちを再現し、実際に開始した FFmpeg プロセスを記録する。

        Args:
            program (str): 起動する実行ファイル
            *args (str): プロセスに渡す引数
            **kwargs (Any): プロセスの入出力などの起動オプション

        Returns:
            asyncio.subprocess.Process: 起動したプロセス
        """
        if program != LIBRARY_PATH['tsreadex']:
            process = await start_process(program, *args, **kwargs)
            if program == LIBRARY_PATH['FFmpeg']:
                encoders.append(process)
            return process

        # 下流の FFmpeg には EOF を届け、tsreadex に相当するプロセスは終了要求まで残す。
        kwargs['stdout'] = asyncio.subprocess.DEVNULL
        process = await start_process(sys.executable, '-c', 'import time; time.sleep(60)', **kwargs)
        wait_for_exit = process.wait

        async def WaitForExit() -> int:
            """
            最初の終了待ちでキャンセルを受け付け、回収時には実際の終了を待つ。

            Args:
                なし

            Returns:
                int: プロセスの終了コード
            """
            if not waiting.is_set():
                waiting.set()
                await asyncio.Future()
            return await wait_for_exit()

        monkeypatch.setattr(process, 'wait', WaitForExit)
        return process

    monkeypatch.setattr(asyncio.subprocess, 'create_subprocess_exec', StartWithStalledTSReadEx)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    try:
        await asyncio.wait_for(waiting.wait(), timeout=10)
        deleted = await asyncio.wait_for(client.delete(url, headers=headers), timeout=5)
        assert deleted.status_code == 204
        assert len(encoders) == 1
        assert encoders[0].returncode is not None
    finally:
        await client.delete(url, headers=headers)
        await asyncio.wait_for(response_task, timeout=5)


@pytest.mark.parametrize(('failure', 'recording'), [
    ('PartialPacket', 'terrestrial-bdav-partial-ts.m2ts'),
    ('IncompletePES', 'terrestrial-bdav-partial-ts.m2ts'),
    ('OversizedPES', 'terrestrial-bdav-partial-ts.m2ts'),
    ('IncompleteInput', 'terrestrial-tsreplace-h264.ts'),
    ('IncompleteMP4', 'terrestrial-psc-sidecar.mp4'),
], indirect=['recording'])
async def test_offline_video_session_failed_encoder_releases_slot(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """
    異常 EOF で正常終端を送らず、再接続と実行枠を解放する。

    Args:
        client (httpx.AsyncClient): テスト用 API クライアント
        recording (tuple[int, Path]): 登録済み録画の ID とパス
        monkeypatch (pytest.MonkeyPatch): 入力または出力の異常終了を再現するために使用
        failure (str): 再現する失敗の種類

    Returns:
        None
    """
    video_id, path = recording
    encoders: set[asyncio.subprocess.Process] = set()
    mp4_muxers: list[asyncio.subprocess.Process] = []
    injected = False
    original_wait = OfflineVideoSession.waitForCapacity
    largest_pending_pes = 0
    test_segment_limit = 16 * 1024
    oversized_pes_continuation = b'\x47\x01\x00\x10' + bytes(ts.PACKET_SIZE - ts.HEADER_SIZE)

    if failure == 'OversizedPES':
        monkeypatch.setattr(OfflineVideoSession, 'MAX_SEGMENT_BYTES', test_segment_limit)
        original_push = PESParser.push

        def TrackPendingPES(parser: PESParser[PES], packet: bytes | bytearray | memoryview) -> None:
            """
            パーサーの動作を保ったまま、未確定の PES の最大サイズを記録する。

            Args:
                parser (PESParser[PES]): 計測対象のパーサー
                packet (bytes | bytearray | memoryview): 入力 TS パケット

            Returns:
                None
            """
            nonlocal largest_pending_pes
            original_push(parser, packet)
            # 録画側のパーサーも同じクラスを使うため、異常を注入した出力パケットを対象とする。
            if packet == oversized_pes_continuation and parser.pes is not None:
                largest_pending_pes = max(largest_pending_pes, len(parser.pes))

        monkeypatch.setattr(PESParser, 'push', TrackPendingPES)

    async def TrackEncoder(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        if encoder not in encoders and failure == 'PartialPacket':
            assert encoder.stdout is not None
            original_read = encoder.stdout.read

            async def ReadWithPartialEOF(size: int = -1) -> bytes:
                nonlocal injected
                chunk = await original_read(size)
                # 実際の変換結果の末尾に、未完成の TS パケットを一つ残す。
                if not chunk and not injected:
                    assert await encoder.wait() == 0
                    injected = True
                    return b'\x47'
                return chunk

            monkeypatch.setattr(encoder.stdout, 'read', ReadWithPartialEOF)
        elif encoder not in encoders and failure in ('IncompletePES', 'OversizedPES'):
            stdout = encoder.stdout
            assert stdout is not None
            original_read = stdout.read

            async def ReadWithBrokenVideoPES(size: int = -1) -> bytes:
                """
                実際のエンコーダー出力の最初の映像 PES に異常を注入する。

                Args:
                    size (int): 元の stdout から読み出す最大サイズ

                Returns:
                    bytes: 異常を注入した TS、またはその続きの正常な TS
                """
                nonlocal injected
                if injected:
                    return await original_read(size)
                prefix = bytearray()
                while len(prefix) < 8 * 1024:
                    packet = await stdout.readexactly(ts.PACKET_SIZE)
                    prefix.extend(packet)
                    if ts.pid(packet) == 0x100 and ts.payload_unit_start_indicator(packet):
                        # FFmpeg の最初の映像 PES だけを書き換える。
                        payload_start = len(prefix) - len(ts.payload(packet))
                        if failure == 'IncompletePES':
                            prefix[payload_start + 4:payload_start + 6] = b'\xff\xff'
                        else:
                            assert prefix[payload_start + 4:payload_start + 6] == b'\x00\x00'
                            prefix.extend(oversized_pes_continuation * 256)
                        injected = True
                        return bytes(prefix)
                raise AssertionError('FFmpeg did not start a video PES in its first 8 KiB.')

            monkeypatch.setattr(stdout, 'read', ReadWithBrokenVideoPES)
        encoders.add(encoder)
        return await original_wait(session, encoder)

    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', TrackEncoder)
    if failure == 'IncompleteInput':
        # 録画のサイズ・更新日時を保ったまま、入力読み取りだけを半分の位置で終了させる。
        eof_offset = path.stat().st_size // 2 // ts.PACKET_SIZE * ts.PACKET_SIZE

        class ShortRecording(io.BufferedReader):
            def read(self, size: int | None = -1) -> bytes:
                nonlocal injected
                remaining = eof_offset - self.tell()
                if remaining <= 0:
                    injected = True
                    return b''
                return super().read(remaining if size is None or size < 0 else min(size, remaining))

        def OpenShortRecording(file_name: str, mode: str) -> io.BufferedReader:
            return ShortRecording(io.FileIO(file_name, mode))

        monkeypatch.setattr(sys.modules[VideoEncodingTask.__module__], 'open', OpenShortRecording, raising=False)
    elif failure == 'IncompleteMP4':
        original_start = asyncio.subprocess.create_subprocess_exec

        async def StartWithShortMP4(program: str, *args: str, **kwargs: Any) -> asyncio.subprocess.Process:
            nonlocal injected
            if program != LIBRARY_PATH['psisimux']:
                return await original_start(program, *args, **kwargs)
            # 約79秒の入力に対し、psisimux が約10秒の正しい TS を出して終了コード 0 で止まる。
            process = await original_start(program, '-u', '10000', *args, **kwargs)
            mp4_muxers.append(process)
            injected = True
            return process

        monkeypatch.setattr(asyncio.subprocess, 'create_subprocess_exec', StartWithShortMP4)

    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    session = await client.put(url)
    assert session.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': session.json()['generation_token']}
    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    # 映像本文中のバイト列と区別し、フレームを解析して正常終端の欠落を確かめる。
    with pytest.raises(struct.error):
        ReadSegments(response.content)
    assert (await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)).status_code == 410
    assert injected is True
    assert len(encoders) == 1
    returncode = next(iter(encoders)).returncode
    if failure in ('IncompletePES', 'OversizedPES'):
        assert returncode is not None
    else:
        assert returncode == 0
    if failure == 'OversizedPES':
        # 次の PES を待ってから失敗すると、すべての継続パケットが PES パーサーに蓄積される。
        assert test_segment_limit < largest_pending_pes <= test_segment_limit + ts.PACKET_SIZE
    if failure == 'IncompleteMP4':
        assert [process.returncode for process in mp4_muxers] == [0]
    async with asyncio.timeout(1):
        async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
            pass
