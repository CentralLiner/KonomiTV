"""オフライン保存の再接続と終了を検証する。"""

import asyncio
import concurrent.futures
import errno
import inspect
import io
import os
import struct
import sys
import threading
import uuid
from collections import Counter
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime
from pathlib import Path
from types import FunctionType
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from biim.mpeg2ts import ts
from biim.mpeg2ts.pes import PES
from starlette.types import Message, Send

from app import schemas
from app.constants import JST, LIBRARY_PATH, PROCESS_POOL_CONTEXT, QUALITY_TYPES
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.schemas import SegmentMapEntry
from app.streams.OfflineVideoSession import (
    OFFLINE_VIDEO_STREAM_SEMAPHORE,
    OfflineVideoSession,
)
from app.streams.VideoEncodingTask import VideoEncodingTask
from app.streams.VideoStream import VideoStream
from tests.conftest import FIXTURES_DIR


@pytest.fixture(scope='module')
def analyzed_recordings() -> dict[Path, schemas.RecordedProgram]:
    """
    同じ録画フィクスチャの解析結果をテスト間で再利用する。

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
    """本番の録画スキャンとは独立に、短い録画をテスト用 DB に登録する。"""
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


def ReadSegments(body: bytes) -> tuple[list[bytes], int]:
    """完全に受信した応答のセグメントと終端件数を読む。"""
    assert body.startswith(b'KTVODLP\n')
    offset = 12 + struct.unpack_from('>I', body, 8)[0]
    segments: list[bytes] = []
    while True:
        sequence, value = struct.unpack_from('>II', body, offset)
        offset += 8
        if sequence == 0xffffffff:
            assert offset == len(body)
            return segments, value
        if sequence == 0xfffffffe:
            assert value == 0
            continue
        assert value > 0
        length = struct.unpack_from('>I', body, offset)[0]
        offset += 4
        assert 0 < length <= OfflineVideoSession.MAX_SEGMENT_BYTES
        segments.append(body[offset:offset + length])
        offset += length


@pytest.mark.asyncio
@pytest.mark.parametrize('recording', [
    'terrestrial-bdav-partial-ts.m2ts',
    'terrestrial-tsreplace-h264.ts',
    'terrestrial-psc-sidecar.mp4',
], indirect=True)
async def test_offline_video_session_replays_the_same_output(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
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
    assert ReadSegments(replay.content) == (segments[1:], count)
    stale_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 1, 'next_sequence': count}, headers=headers)
    assert stale_ack.status_code == 409
    future_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 2, 'next_sequence': count + 1}, headers=headers)
    assert future_ack.status_code == 409
    smaller_ack = await client.put(f'{url}/checkpoint', json={'connection_id': 2, 'next_sequence': 0}, headers=headers)
    assert smaller_ack.json()['next_sequence'] == 1
    assert (await client.get(f'{url}?connection_id=1&next_sequence=1', headers=headers)).status_code == 409
    assert (await client.get(f'{url}?connection_id=3&next_sequence=0', headers=headers)).status_code == 410

    end = await client.get(f'{url}?connection_id=4&next_sequence={count}', headers=headers)
    assert ReadSegments(end.content) == ([], count)
    assert (await client.delete(url, headers=headers)).status_code == 204
    assert (await client.delete(url, headers=headers)).status_code == 204
    assert (await client.get(f'{url}?connection_id=5&next_sequence={count}', headers=headers)).status_code == 410
    replacement = await client.put(url)
    assert replacement.status_code == 201
    assert replacement.json()['generation_token'] != created.json()['generation_token']
    assert (await client.delete(url, headers=headers)).status_code == 409


@pytest.mark.asyncio
async def test_offline_video_session_replays_after_recording_io_error(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """録画の一時的な I/O 障害から、同じセッションの未保存区間と終端を取り直せる。"""
    video_id, path = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count > 1

    original_stat = Path.stat
    calls_until_failure = 1

    def StatWithTemporaryFailure(target: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        nonlocal calls_until_failure
        if target == path and calls_until_failure > 0:
            calls_until_failure -= 1
            if calls_until_failure == 0:
                raise OSError(errno.EIO, 'Temporary recording I/O error')
        return original_stat(target, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, 'stat', StatWithTemporaryFailure)
    # 再接続時の照合で一度失敗しても、次の GET で既存のデータを取り直せる。
    retry = await client.get(f'{url}?connection_id=2&next_sequence=1', headers=headers)
    assert retry.status_code == 503
    replay = await client.get(f'{url}?connection_id=3&next_sequence=1', headers=headers)
    assert ReadSegments(replay.content) == (segments[1:], count)

    # GET の照合は通し、正常終端を公開する直前の照合で失敗させる。
    calls_until_failure = 2
    interrupted = await client.get(f'{url}?connection_id=4&next_sequence={count}', headers=headers)
    assert interrupted.status_code == 200
    with pytest.raises(struct.error):
        ReadSegments(interrupted.content)
    end = await client.get(f'{url}?connection_id=5&next_sequence={count}', headers=headers)
    assert ReadSegments(end.content) == ([], count)


@pytest.mark.asyncio
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
    """分割境界にある最終フレームを、主音声・副音声と同じ末尾セグメントに保持する。"""
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


@pytest.mark.asyncio
@pytest.mark.parametrize('recording', ['terrestrial-tsreplace-h264.ts'], indirect=True)
async def test_offline_video_session_allows_slow_segment_map_save(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_save = VideoStream.saveSegmentMapEntries
    delayed = False

    async def SaveWithDelay(stream: VideoStream, entries: list[SegmentMapEntry]) -> None:
        nonlocal delayed
        # エンコード開始時の単一エントリ保存を済ませ、配信中の保存で DB 待ちを再現する。
        if len(entries) > 1 and not delayed:
            delayed = True
            await asyncio.sleep(10.2)
        await original_save(stream, entries)

    # 短い録画でもエンコード中に保存が実行されるよう、バッチ閾値を下げる。
    monkeypatch.setattr(VideoStream, 'SEGMENT_MAP_SAVE_BATCH_SIZE', 1)
    monkeypatch.setattr(VideoStream, 'saveSegmentMapEntries', SaveWithDelay)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}

    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert delayed is True
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count >= 2


@pytest.mark.asyncio
async def test_offline_video_session_keeps_waiting_encoder_on_disconnect(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


@pytest.mark.asyncio
async def test_offline_video_session_reconnects_while_waiting_for_capacity(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """未確認データと送信中の参照を保持し、再接続後の ACK で同じエンコーダーを再開する。"""
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


@pytest.mark.asyncio
@pytest.mark.parametrize('waiting_for', ['SourcePosition', 'SegmentCapacity', 'SegmentMapSave', 'BeforePipeRegistration', 'AfterPipeRegistration'])
async def test_offline_video_session_cancels_pending_encoding(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    waiting_for: str,
) -> None:
    """入力の探索・パイプの登録・容量待ちの途中で DELETE が資源と実行枠を解放する。"""
    waiting = asyncio.Event()
    release = asyncio.Event()
    encoder_process: asyncio.subprocess.Process | None = None
    capacity_wait_count = 0
    segment_map_save_count = 0
    original_resolve = VideoStream.resolveSegmentSourcePosition
    original_wait = OfflineVideoSession.waitForCapacity
    original_save = VideoStream.saveSegmentMapEntries
    original_to_thread = anyio.to_thread.run_sync
    original_pipe = os.pipe
    pipe_fds: list[int] = []

    async def ResolveWithDelay(stream: VideoStream, sequence: int) -> None:
        waiting.set()
        await release.wait()
        await original_resolve(stream, sequence)

    async def WaitWithDelay(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        nonlocal encoder_process, capacity_wait_count
        encoder_process = encoder
        capacity_wait_count += 1
        # 1件生成した後で待ち、保持中のデータと実プロセスの回収も確認する。
        if capacity_wait_count == 2:
            waiting.set()
            await release.wait()
        return await original_wait(session, encoder)

    async def SaveWithDelay(stream: VideoStream, entries: list[SegmentMapEntry]) -> None:
        nonlocal segment_map_save_count
        segment_map_save_count += 1
        # 配信中の DB 待ちをキャンセルした後も、同じ保存先の待機は継続している状況を再現する。
        if segment_map_save_count > 1:
            waiting.set()
            await release.wait()
        await original_save(stream, entries)

    def TrackPipe() -> tuple[int, int]:
        pipe = original_pipe()
        pipe_fds.extend(pipe)
        return pipe

    async def DelayPipeRegistration[T](func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        if func.__name__ == '__registerTSReadExInputPipe':
            # 登録前と登録後の両方で、所有権の受け渡し中にキャンセルする。
            if waiting_for == 'AfterPipeRegistration':
                result = await original_to_thread(func, *args, **kwargs)
                waiting.set()
                await release.wait()
                return result
            waiting.set()
            await release.wait()
        return await original_to_thread(func, *args, **kwargs)

    if waiting_for == 'SourcePosition':
        monkeypatch.setattr(VideoStream, 'resolveSegmentSourcePosition', ResolveWithDelay)
    elif waiting_for == 'SegmentCapacity':
        monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', WaitWithDelay)
    elif waiting_for == 'SegmentMapSave':
        monkeypatch.setattr(VideoStream, 'SEGMENT_MAP_SAVE_BATCH_SIZE', 1)
        monkeypatch.setattr(VideoStream, 'saveSegmentMapEntries', SaveWithDelay)
    else:
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
        if pipe_fds:
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
        if encoder_process is not None:
            assert encoder_process.returncode is not None
    finally:
        # 修正前のコードで失敗した場合も、待機を解除してテスト用の資源を回収する。
        release.set()
        await client.delete(url, headers=headers)
        await response_task


@pytest.mark.asyncio
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
    loop = asyncio.get_running_loop()
    queued_feed: asyncio.Future[bool] = loop.create_future()
    waiting_for_feed = asyncio.Event()
    pipe_fd: int | None = None
    original_run_in_executor = loop.run_in_executor
    original_to_thread = anyio.to_thread.run_sync
    original_wait_for = asyncio.wait_for

    async def TrackRegistration[T](func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        nonlocal pipe_fd
        if func.__name__ == '__registerTSReadExInputPipe':
            pipe_fd = args[0]
        return await original_to_thread(func, *args, **kwargs)

    def QueueFeed(executor: concurrent.futures.Executor | None, func: Callable[..., Any], *args: Any) -> asyncio.Future[Any]:
        # 入力ワーカーが使用中で、フィードがまだキューにある状態を作る。
        if isinstance(func, FunctionType) and func.__name__ == 'FeedTSStream':
            return queued_feed
        return original_run_in_executor(executor, func, *args)

    async def WaitFor[T](awaitable: Awaitable[T], timeout: float | None) -> T:
        if awaitable is queued_feed:
            waiting_for_feed.set()
        return await original_wait_for(awaitable, timeout)

    async def FailEncoding(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        raise RuntimeError('Encoder failed before the input worker started.')

    monkeypatch.setattr(anyio.to_thread, 'run_sync', TrackRegistration)
    monkeypatch.setattr(loop, 'run_in_executor', QueueFeed)
    monkeypatch.setattr(asyncio, 'wait_for', WaitFor)
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', FailEncoding)

    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response_task = asyncio.create_task(client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers))
    try:
        await asyncio.wait_for(waiting_for_feed.wait(), timeout=10)
        assert pipe_fd is not None
        assert (await client.delete(url, headers=headers)).status_code == 204
        with pytest.raises(OSError):
            os.fstat(pipe_fd)
    finally:
        await client.delete(url, headers=headers)
        await response_task
        # 修正前のコードで漏れを検出した場合も、テスト側で残った FD を閉じる。
        if pipe_fd is not None:
            try:
                os.close(pipe_fd)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_offline_video_session_accepts_stale_database_mtime(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
    """保存前の日時だけの差は許容し、保存開始後の実ファイルの変更は拒否する。"""
    video_id, path = recording
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}
    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count > 0

    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000_000))
    response = await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)
    assert response.status_code == 409
    assert response.json()['detail']['code'] == 'recording_changed'


@pytest.mark.asyncio
async def test_offline_video_session_sends_keepalive_during_final_validation(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """終端照合の I/O 待ちにも keepalive を送り、照合後だけ正常終端を公開する。"""
    checking = asyncio.Event()
    release = asyncio.Event()
    keepalive = asyncio.Event()
    original_stream = OfflineVideoSession.stream

    async def CheckWithDelay(_session: OfflineVideoSession) -> bool:
        checking.set()
        await asyncio.wait_for(release.wait(), 10)
        return True

    async def TrackStream(session: OfflineVideoSession, connection_id: int, next_sequence: int, send: Send) -> None:
        # 初回 GET の照合が済んだ時点で、正常終端の公開前の照合に待ち時間を入れる。
        monkeypatch.setattr(OfflineVideoSession, '_matchesRecording', CheckWithDelay)

        async def TrackSend(message: Message) -> None:
            if checking.is_set() and message.get('body') == struct.pack('>II', 0xfffffffe, 0):
                keepalive.set()
            await send(message)

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
    finally:
        release.set()
        await client.delete(url, headers=headers)
        await response_task


@pytest.mark.asyncio
@pytest.mark.parametrize('recording', [
    'terrestrial-bdav-partial-ts.m2ts',
    'terrestrial-tsreplace-h264.ts',
    'terrestrial-psc-sidecar.mp4',
], indirect=True)
async def test_offline_video_session_legacy_stream(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
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

@pytest.mark.asyncio
@pytest.mark.parametrize(('phase', 'recording'), [
    ('Probe', 'terrestrial-bdav-partial-ts.m2ts'),
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
    """入力が停止していても、キャンセルや起動失敗から実行枠を回収する。"""
    video_id, path = recording
    loop = asyncio.get_running_loop()
    read_started = asyncio.Event()
    file_closed = asyncio.Event()
    encoder_started = asyncio.Event()
    release_read = threading.Event()
    encoders: list[asyncio.subprocess.Process] = []
    original_wait = OfflineVideoSession.waitForCapacity

    class StalledFile(io.FileIO):
        did_block = False

        def readinto(self, buffer: Any) -> int | None:
            # BufferedReader が内部ロックを保持したまま、下層の read() を待つ状況を作る。
            frame = inspect.currentframe()
            is_feed = False
            while frame is not None:
                if frame.f_code.co_name in ('FeedTSStream', 'FeedOfflineMP4Stream'):
                    is_feed = True
                    break
                frame = frame.f_back
            if not self.did_block and is_feed == (phase != 'Probe'):
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
        assert Path(file_name) == path and mode == 'rb'
        return io.BufferedReader(StalledFile(file_name, mode))

    async def TrackEncoder(session: OfflineVideoSession, encoder: asyncio.subprocess.Process) -> bool:
        encoders.append(encoder)
        encoder_started.set()
        return await original_wait(session, encoder)

    if path.suffix == '.mp4':
        original_fdopen = os.fdopen

        def OpenMP4Input(fd: int, *args: Any, **kwargs: Any) -> Any:
            # psisimux の読み込みパイプだけを、停止する BufferedReader に差し替える。
            frame = inspect.currentframe()
            caller = frame.f_back if frame is not None else None
            if caller is not None and caller.f_code is VideoEncodingTask.run.__code__:
                return io.BufferedReader(StalledFile(fd, 'rb'))
            return original_fdopen(fd, *args, **kwargs)

        monkeypatch.setattr(os, 'fdopen', OpenMP4Input)
    else:
        monkeypatch.setattr(sys.modules[VideoEncodingTask.__module__], 'open', OpenInput, raising=False)
    monkeypatch.setattr(OfflineVideoSession, 'waitForCapacity', TrackEncoder)
    # 入力 read() 一つで既定プールを満杯にし、回収が同じプールの空きを待たないことを確認する。
    input_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    original_run = loop.run_in_executor

    def RunWithLimitedInputPool(executor: concurrent.futures.Executor | None, func: Callable[..., Any], *args: Any) -> asyncio.Future[Any]:
        return original_run(input_executor if executor is None else executor, func, *args)

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
        if phase != 'Probe':
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
        input_executor.shutdown(wait=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('same_id', [False, True], ids=['DifferentIDs', 'SameID'])
async def test_offline_video_session_put_does_not_read_recording(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    same_id: bool,
) -> None:
    """同時 PUT は I/O なしで登録を処理し、ファイルハッシュは初回 GET で照合する。"""
    video_id, _ = recording
    original_hash = MetadataAnalyzer.matchesFileHash
    hash_checks = 0

    def CountHashChecks(analyzer: MetadataAnalyzer, file_hash: str) -> bool:
        nonlocal hash_checks
        hash_checks += 1
        return original_hash(analyzer, file_hash)

    monkeypatch.setattr(MetadataAnalyzer, 'matchesFileHash', CountHashChecks)
    first_id = uuid.uuid4().hex
    urls = [
        f'/api/streams/video/{video_id}/240p/offline-sessions/{first_id if same_id else uuid.uuid4().hex}'
        for _ in range(12)
    ]
    responses = await asyncio.gather(*(client.put(url) for url in urls))
    assert hash_checks == 0
    assert sorted(response.status_code for response in responses) == ([200] * 11 + [201] if same_id else [201] * 3 + [429] * 9)
    assert len({response.json()['generation_token'] for response in responses if response.status_code != 429}) == (1 if same_id else 3)

    created_index = next(index for index, response in enumerate(responses) if response.status_code == 201)
    headers = {'X-KonomiTV-Offline-Generation': responses[created_index].json()['generation_token']}
    response = await client.get(f'{urls[created_index]}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count >= 1
    replay = await client.get(f'{urls[created_index]}?connection_id=2&next_sequence={count}', headers=headers)
    assert ReadSegments(replay.content) == ([], count)
    assert hash_checks == 1


@pytest.mark.asyncio
async def test_offline_video_session_capacity_and_recording_change(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['InitialProbe', 'RetryLimit', 'AfterPublication'])
async def test_offline_video_session_retries_probe_only_before_publication(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """公開前の解析失敗だけを再試行し、上限到達と公開後の失敗では正常終端を送らない。"""
    probe_durations: list[int] = []
    published_sequences: list[int] = []
    original_build = VideoEncodingTask.buildFFmpegOptions
    original_publish = OfflineVideoSession.publishSegment

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
        elif failure == 'RetryLimit' or len(probe_durations) == 1:
            # 解析時に音声を検出できなかった場合と同じ、FFmpeg の起動エラーを発生させる。
            options[options.index('0:a:1')] = '0:a:99'
        return options

    def TrackPublication(session: OfflineVideoSession, sequence: int, data: bytearray, duration_seconds: float) -> None:
        original_publish(session, sequence, data, duration_seconds)
        published_sequences.append(sequence)

    monkeypatch.setattr(VideoEncodingTask, 'MAX_RETRY_COUNT', 2)
    monkeypatch.setattr(VideoEncodingTask, 'buildFFmpegOptions', BuildWithProbeFailure)
    monkeypatch.setattr(OfflineVideoSession, 'publishSegment', TrackPublication)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/240p/offline-sessions/{uuid.uuid4().hex}'
    created = await client.put(url)
    assert created.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': created.json()['generation_token']}

    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    assert len(probe_durations) == (1 if failure == 'AfterPublication' else 2)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(('failure', 'recording'), [
    ('EncoderError', 'terrestrial-bdav-partial-ts.m2ts'),
    ('PartialPacket', 'terrestrial-bdav-partial-ts.m2ts'),
    ('IncompleteInput', 'terrestrial-tsreplace-h264.ts'),
    ('IncompleteMP4', 'terrestrial-psc-sidecar.mp4'),
], indirect=['recording'])
async def test_offline_video_session_failed_encoder_releases_slot(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """変換の例外と異常 EOF で正常終端を送らず、再接続と実行枠を解放する。"""
    video_id, path = recording
    encoders: set[asyncio.subprocess.Process] = set()
    mp4_muxers: list[asyncio.subprocess.Process] = []
    injected = False
    original_wait = OfflineVideoSession.waitForCapacity

    async def FailEncoding(self: VideoStream) -> None:
        await asyncio.sleep(0.05)
        raise RuntimeError('Injected encoder failure')

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
        encoders.add(encoder)
        return await original_wait(session, encoder)

    if failure == 'EncoderError':
        monkeypatch.setattr(VideoStream, 'encodeOffline', FailEncoding)
    else:
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
    if failure != 'EncoderError':
        assert injected is True
        assert len(encoders) == 1
        assert next(iter(encoders)).returncode == 0
    if failure == 'IncompleteMP4':
        assert [process.returncode for process in mp4_muxers] == [0]
    async with asyncio.timeout(1):
        async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
            pass
