"""オフライン保存の再接続と終了を検証する。"""

import asyncio
import struct
import uuid
from collections import Counter
from collections.abc import AsyncGenerator
from datetime import datetime
from pathlib import Path

import httpx2 as httpx
import pytest
from biim.mpeg2ts import ts
from starlette.types import Message

from app.constants import JST, QUALITY_TYPES
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.streams.OfflineVideoSession import (
    OFFLINE_VIDEO_STREAM_SEMAPHORE,
    OfflineVideoSession,
)
from app.streams.VideoEncodingTask import VideoEncodingTask
from app.streams.VideoStream import VideoStream
from tests.conftest import FIXTURES_DIR


@pytest.fixture(params=['terrestrial-bdav-partial-ts.m2ts'], ids=['M2TS'])
async def recording(client: httpx.AsyncClient, tmp_path: Path, request: pytest.FixtureRequest) -> AsyncGenerator[tuple[int, Path]]:
    """本番の録画スキャンとは独立に、短い録画をテスト用 DB に登録する。"""
    file_name: str = request.param
    source = FIXTURES_DIR / 'recorded' / file_name
    path = tmp_path / source.name
    path.write_bytes(source.read_bytes())
    if path.suffix == '.mp4':
        path.with_suffix('.psc').write_bytes(source.with_suffix('.psc').read_bytes())
    stat = path.stat()
    analyzed = await asyncio.to_thread(MetadataAnalyzer(path).analyze)
    assert analyzed is not None
    program_fields = analyzed.model_dump(exclude={'id', 'channel', 'recorded_video', 'series_id', 'series_broadcast_period_id'})
    program = await RecordedProgram.create(**program_fields)
    video_fields = analyzed.recorded_video.model_dump(exclude={'id', 'recorded_program_id'})
    # 仮想プレイリストの範囲を意図的に短くし、実出力から分割数と末尾時間を求めることも検証する。
    video_fields.update(file_size=stat.st_size, file_modified_at=datetime.fromtimestamp(stat.st_mtime, tz=JST), duration=1.0)
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
@pytest.mark.parametrize(('quality', 'frame_rate', 'frame_count'), [
    ('1080p-60fps', 60000, 361),
    ('720p', 30000, 181),
    ('540p-24fps', 24000, 145),
])
async def test_offline_video_session_keeps_final_boundary_frame_with_audio(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
    quality: str,
    frame_rate: int,
    frame_count: int,
) -> None:
    """分割境界にある最終フレームを、主音声・副音声と同じ末尾セグメントに保持する。"""
    original_build = VideoEncodingTask.buildFFmpegOptions

    def BuildWithFinalKeyFrame(task: VideoEncodingTask, quality: QUALITY_TYPES, output_ts_offset: float) -> list[str]:
        options = original_build(task, quality, output_ts_offset)
        # 実録画を最後まで読み、6.006 秒の分割境界に最後のキーフレームを配置する。
        options[options.index('-vf') + 1] += f',fps={frame_rate}/1001,trim=end_frame={frame_count},setpts=PTS-STARTPTS'
        options[options.index('-af') + 1] += f',asetpts=PTS-STARTPTS,atrim=duration={frame_count * 1001 / frame_rate}'
        if '-r' in options:
            rate_index = options.index('-r')
            del options[rate_index:rate_index + 2]
        options[-1:-1] = ['-sc_threshold', '0', '-force_key_frames', f'expr:eq(n,{frame_count - 1})', '-fps_mode', 'passthrough']
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
    for offset in range(0, len(segments[0]), ts.PACKET_SIZE):
        packet = segments[0][offset:offset + ts.PACKET_SIZE]
        pid = ts.pid(packet)
        packet_counts[pid] += 1
        if pid == 0x100 and ts.payload_unit_start_indicator(packet):
            video_frames += 1
    assert video_frames == frame_count
    assert packet_counts[0x101] > 0
    assert packet_counts[0x102] > 0


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
async def test_offline_video_session_legacy_stream(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
) -> None:
    video_id, _ = recording
    response = await client.get(f'/api/streams/video/{video_id}/240p/offline-stream')
    assert response.status_code == 200
    segments, count = ReadSegments(response.content)
    assert len(segments) == count == 1

    playlist = await client.get(f'/api/streams/video/{video_id}/240p/playlist?session_id={uuid.uuid4().hex}')
    assert playlist.status_code == 200
    segment_url = next(line for line in playlist.text.splitlines() if line.startswith('segment?'))
    segment = await client.get(f'/api/streams/video/{video_id}/240p/{segment_url}')
    assert segment.status_code == 200
    assert segment.content.startswith(b'\x47')


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
async def test_offline_video_session_failed_encoder_releases_slot(
    client: httpx.AsyncClient,
    recording: tuple[int, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def FailEncoding(self: VideoStream) -> None:
        await asyncio.sleep(0.05)
        raise RuntimeError('Injected encoder failure')

    monkeypatch.setattr(VideoStream, 'encodeOffline', FailEncoding)
    video_id, _ = recording
    url = f'/api/streams/video/{video_id}/720p/offline-sessions/{uuid.uuid4().hex}'
    session = await client.put(url)
    assert session.status_code == 201
    headers = {'X-KonomiTV-Offline-Generation': session.json()['generation_token']}
    response = await client.get(f'{url}?connection_id=1&next_sequence=0', headers=headers)
    assert response.status_code == 200
    assert b'\xff\xff\xff\xff' not in response.content
    assert (await client.get(f'{url}?connection_id=2&next_sequence=0', headers=headers)).status_code == 410
    async with asyncio.timeout(1):
        async with OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE, OFFLINE_VIDEO_STREAM_SEMAPHORE:
            pass
