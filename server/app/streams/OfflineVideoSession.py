from __future__ import annotations

import asyncio
import secrets
import struct
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from anyio import CancelScope, move_on_after
from fastapi import HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import ClientDisconnect
from starlette.types import Message, Receive, Scope, Send

from app import logging
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.schemas import OfflineVideoSessionInfo, OfflineVideoSessionStreamMetadata
from app.streams.StreamEncodingOptions import StreamQualityWithOptions
from app.streams.VideoStream import VideoStream


OFFLINE_VIDEO_STREAM_SEMAPHORE = asyncio.Semaphore(3)


def OfflineVideoSessionError(status_code: int, code: str, retry_after: int | None = None) -> HTTPException:
    """オフライン保存クライアントが判別するエラーを返す。"""
    return HTTPException(
        status_code,
        detail = {'code': code},
        headers = {'Retry-After': str(retry_after)} if retry_after is not None else None,
    )


class OfflineVideoSession:
    """エンコーダー、未確認の TS データ、HTTP 接続を所有する。"""

    MAX_SESSIONS: ClassVar[int] = 3
    # 確定済み TS に加え、生成中の配列と bytes 変換にそれぞれ最大 32 MiB を使う。
    MAX_SEGMENT_BYTES: ClassVar[int] = 32 * 1024 * 1024
    RETAINED_BYTES_LIMIT: ClassVar[int] = 64 * 1024 * 1024
    LEASE_TIMEOUT: ClassVar[int] = 90
    KEEPALIVE_INTERVAL: ClassVar[float] = 10.0
    _sessions: ClassVar[dict[str, OfflineVideoSession]] = {}

    def __init__(
        self,
        session_id: str,
        recorded_program: RecordedProgram,
        quality: str,
        stream_quality: StreamQualityWithOptions,
    ) -> None:
        """
        Args:
            session_id (str): クライアントが指定した保存 ID
            recorded_program (RecordedProgram): 作成時点の録画情報
            quality (str): 追加オプションを含む要求画質
            stream_quality (StreamQualityWithOptions): エンコーダーへ渡す画質
        """
        self.session_id = session_id
        self._recorded_program = recorded_program
        self._stream_quality = stream_quality
        # 実ファイルの照合は最初の GET で行い、以後の接続と正常終端で比較する。
        self._file_identity: tuple[int, int, int, int, int] | None = None
        self.info = OfflineVideoSessionInfo(
            video_id = recorded_program.id,
            file_hash = recorded_program.recorded_video.file_hash,
            quality = quality,
            duration_seconds = recorded_program.recorded_video.duration,
            generation_token = secrets.token_hex(16),
        )

        # 確定した TS と再生時間を再送にも使う。送信中は ACK 後も参照数を数える。
        self._retained: dict[int, tuple[bytes, float]] = {}
        self._sending: dict[int, int] = {}
        self._retained_bytes = 0
        self._acked_next = 0
        self._offered_next = 0
        self._produced_next = 0
        self._changed = asyncio.Event()
        self._is_complete = False

        # 接続番号は切断後も保持し、遅れて届いた GET が接続を奪うことを防ぐ。
        self._connection_id = 0
        self._response_scope: CancelScope | None = None
        self._stream: VideoStream | None = None
        self._encoding_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._lease_deadline = 0.0
        self._lease_timer: asyncio.TimerHandle | None = None
        self._renewLease()

    @classmethod
    def create(
        cls,
        session_id: str,
        recorded_program: RecordedProgram,
        quality: str,
        stream_quality: StreamQualityWithOptions,
    ) -> tuple[OfflineVideoSession, bool]:
        """録画情報と画質を確認し、同じ保存 ID の並行作成を一つの登録にまとめる。"""
        recorded_video = recorded_program.recorded_video
        if recorded_video.status != 'Recorded':
            raise OfflineVideoSessionError(409, 'recording_unavailable')

        # ファイル I/O や await を挟まず、上限確認と登録を一度に行う。
        existing = cls._sessions.get(session_id)
        if existing is not None:
            existing._validateTarget(recorded_program.id, quality)
            if existing._recordingFields(recorded_video) != existing._recordingFields(existing._recorded_program.recorded_video):
                raise OfflineVideoSessionError(409, 'recording_changed')
            if existing._stream_quality != stream_quality:
                raise OfflineVideoSessionError(409, 'session_mismatch')
            if existing.is_closed:
                raise OfflineVideoSessionError(429, 'session_capacity', retry_after=5)
            return existing, False
        if len(cls._sessions) >= cls.MAX_SESSIONS:
            raise OfflineVideoSessionError(429, 'session_capacity', retry_after=5)

        session = cls(session_id, recorded_program, quality, stream_quality)
        cls._sessions[session_id] = session
        return session, True

    @classmethod
    def get(
        cls,
        session_id: str,
        video_id: int,
        quality: str,
        generation_token: str | None,
    ) -> OfflineVideoSession | None:
        """登録済みの対象と世代を照合する。"""
        session = cls._sessions.get(session_id)
        if session is not None:
            session._validateTarget(video_id, quality)
            if generation_token is None or not secrets.compare_digest(
                session.info.generation_token.encode('ascii'),
                generation_token.encode('utf-8'),
            ):
                raise OfflineVideoSessionError(409, 'generation_mismatch')
        return session

    @property
    def is_closed(self) -> bool:
        """回収が始まっていれば、新しい要求を失効として扱う。"""
        if self._close_task is None and asyncio.get_running_loop().time() >= self._lease_deadline:
            self.close()
        return self._close_task is not None

    @classmethod
    async def closeAll(cls) -> None:
        """サーバー終了時に全セッションの回収を待つ。"""
        await asyncio.gather(*(session.close() for session in tuple(cls._sessions.values())))

    def close(self) -> asyncio.Task[None]:
        """接続と待機を解放し、同じ回収タスクを返す。"""
        if self._close_task is None:
            if self._lease_timer is not None:
                self._lease_timer.cancel()
            if self._response_scope is not None:
                self._response_scope.cancel()
            self._changed.set()
            self._close_task = asyncio.create_task(self._cleanup())
        return self._close_task

    async def connect(self, connection_id: int, next_sequence: int) -> None:
        """同一世代へ接続し、進捗を確定して旧 HTTP 応答を中断する。"""
        self._validateConnection(connection_id, is_new=True)
        matches_recording = await self._matchesRecording()
        self._validateConnection(connection_id, is_new=True)
        if not matches_recording:
            self.close()
            raise OfflineVideoSessionError(409, 'recording_changed')

        # ファイルと DB の照合中に別の接続や期限切れが入る可能性がある。
        if next_sequence < self._acked_next:
            raise OfflineVideoSessionError(410, 'segment_unavailable')
        self._acknowledge(next_sequence)
        self._connection_id = connection_id
        if self._response_scope is not None:
            self._response_scope.cancel()
            self._response_scope = None

    def checkpoint(self, connection_id: int, next_sequence: int) -> int:
        """現接続からの保存位置を累積確認し、lease を更新する。"""
        self._validateConnection(connection_id, is_new=False)
        self._acknowledge(next_sequence)
        return self._acked_next

    async def waitForCapacity(self, encoder: asyncio.subprocess.Process) -> bool:
        """未確認データに最大サイズのセグメントを一つ追加できるまで待つ。"""
        while self._close_task is None:
            if encoder.returncode not in (None, 0):
                raise RuntimeError('Encoder exited while waiting for offline segment capacity.')
            if self._retained_bytes + self.MAX_SEGMENT_BYTES <= self.RETAINED_BYTES_LIMIT:
                return True
            # 容量待ちでも、プロセスが出力できなくなった場合は失敗を伝える。
            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=1.0)
            except TimeoutError:
                pass
        return False

    def publishSegment(self, sequence: int, data: bytearray, duration_seconds: float) -> None:
        """
        一度だけ bytes に変換し、送信と再送が参照するデータを確定する。

        Args:
            sequence (int): 確定するセグメントの連続番号
            data (bytearray): エンコード済み TS
            duration_seconds (float): セグメントの再生時間
        """
        assert sequence == self._produced_next
        assert 0 < len(data) <= self.MAX_SEGMENT_BYTES
        assert self._retained_bytes + len(data) <= self.RETAINED_BYTES_LIMIT
        self._retained[sequence] = (bytes(data), duration_seconds)
        self._retained_bytes += len(data)
        self._produced_next += 1
        self._changed.set()

    async def stream(self, connection_id: int, next_sequence: int, send: Send) -> None:
        """現接続のメタデータ、セグメント、終端を順に送る。"""
        metadata = OfflineVideoSessionStreamMetadata(
            **self.info.model_dump(),
            session_id = self.session_id,
            next_sequence = next_sequence,
        ).model_dump_json().encode('utf-8')
        await send({
            'type': 'http.response.body',
            'body': b'KTVODLP\n' + struct.pack('>I', len(metadata)) + metadata,
            'more_body': True,
        })
        loop = asyncio.get_running_loop()
        keepalive_deadline = loop.time() + self.KEEPALIVE_INTERVAL
        sequence = next_sequence
        while self._close_task is None and connection_id == self._connection_id:
            segment = self._retained.get(sequence)
            if segment is not None:
                # ASGI send の完了を待つ間に ACK が届いても、本体への参照を予算に含める。
                data, duration_seconds = segment
                self._sending[sequence] = self._sending.get(sequence, 0) + 1
                try:
                    header = struct.pack('>III', sequence, max(1, round(duration_seconds * 1000)), len(data))
                    await send({'type': 'http.response.body', 'body': header, 'more_body': True})
                    if self._close_task is not None or connection_id != self._connection_id:
                        return
                    self._offered_next = max(self._offered_next, sequence + 1)
                    await send({'type': 'http.response.body', 'body': data, 'more_body': True})
                finally:
                    del data, segment
                    self._sending[sequence] -= 1
                    if self._sending[sequence] == 0:
                        del self._sending[sequence]
                        if sequence < self._acked_next:
                            self._releaseSegment(sequence)
                        self._changed.set()
                sequence += 1
                keepalive_deadline = loop.time() + self.KEEPALIVE_INTERVAL
                continue

            if self._is_complete:
                assert sequence == self._produced_next
                if await self._matchesRecording():
                    if self._close_task is None and connection_id == self._connection_id:
                        await send({'type': 'http.response.body', 'body': struct.pack('>II', 0xffffffff, sequence), 'more_body': True})
                elif self._close_task is None and connection_id == self._connection_id:
                    self.close()
                return

            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=keepalive_deadline - loop.time())
            except TimeoutError:
                if self._close_task is None and connection_id == self._connection_id:
                    await send({'type': 'http.response.body', 'body': struct.pack('>II', 0xfffffffe, 0), 'more_body': True})
                keepalive_deadline = loop.time() + self.KEEPALIVE_INTERVAL

    def bindResponse(self, connection_id: int, scope: CancelScope) -> bool:
        """レスポンス開始時に、現接続として中断できるよう登録する。"""
        if self.is_closed or self._connection_id != connection_id:
            return False
        self._response_scope = scope
        if self._encoding_task is None:
            self._encoding_task = asyncio.create_task(self._encode())
        return True

    def unbindResponse(self, connection_id: int, scope: CancelScope) -> None:
        """終了した応答の中断スコープを外す。"""
        if self._connection_id == connection_id and self._response_scope is scope:
            self._response_scope = None

    def _validateTarget(self, video_id: int, quality: str) -> None:
        if self.info.video_id != video_id or self.info.quality != quality:
            raise OfflineVideoSessionError(409, 'session_mismatch')

    def _validateConnection(self, connection_id: int, *, is_new: bool) -> None:
        if self.is_closed:
            raise OfflineVideoSessionError(410, 'session_unavailable')
        if is_new and connection_id <= self._connection_id:
            raise OfflineVideoSessionError(409, 'stale_connection')
        if not is_new and (self._connection_id == 0 or connection_id != self._connection_id):
            raise OfflineVideoSessionError(409, 'stale_connection')

    def _acknowledge(self, next_sequence: int) -> None:
        if next_sequence > self._offered_next:
            raise OfflineVideoSessionError(409, 'invalid_checkpoint')
        for sequence in range(self._acked_next, next_sequence):
            if sequence not in self._sending:
                self._releaseSegment(sequence)
        self._acked_next = max(self._acked_next, next_sequence)
        self._changed.set()
        self._renewLease()

    def _renewLease(self) -> None:
        if self._lease_timer is not None:
            self._lease_timer.cancel()
        loop = asyncio.get_running_loop()
        self._lease_deadline = loop.time() + self.LEASE_TIMEOUT
        self._lease_timer = loop.call_at(self._lease_deadline, self.close)

    def _releaseSegment(self, sequence: int) -> None:
        data, _ = self._retained.pop(sequence)
        self._retained_bytes -= len(data)

    @staticmethod
    def _recordingFields(video: RecordedVideo) -> tuple[int, str, str, int, datetime]:
        return video.id, video.file_path, video.file_hash, video.file_size, video.file_modified_at

    @staticmethod
    def _getFileIdentity(video: RecordedVideo) -> tuple[int, int, int, int, int] | None:
        # ハッシュの計算とファイル操作はワーカースレッドで行う。
        path = Path(video.file_path)
        try:
            before = path.stat()
            if before.st_size != video.file_size:
                return None
            if datetime.fromtimestamp(before.st_mtime, tz=video.file_modified_at.tzinfo) != video.file_modified_at:
                return None
            if not MetadataAnalyzer(path).matchesFileHash(video.file_hash):
                return None
            after = path.stat()
        except (OSError, ValueError):
            return None
        identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            return None
        return identity

    async def _matchesRecording(self) -> bool:
        recorded_program = await RecordedProgram.filter(id=self.info.video_id).get_or_none().select_related('recorded_video')
        if recorded_program is None or recorded_program.recorded_video.status != 'Recorded':
            return False
        if self._recordingFields(recorded_program.recorded_video) != self._recordingFields(self._recorded_program.recorded_video):
            return False
        file_identity = await asyncio.to_thread(self._getFileIdentity, recorded_program.recorded_video)
        if file_identity is None:
            return False
        if self._file_identity is None:
            self._file_identity = file_identity
        return self._file_identity == file_identity

    async def _encode(self) -> None:
        try:
            # GET の応答は待機中もメタデータと keepalive を送信できる。
            async with OFFLINE_VIDEO_STREAM_SEMAPHORE:
                if self._close_task is not None:
                    return
                assert self._stream_quality.quality != 'original'
                self._stream = VideoStream(
                    f'offline-ram-{self.session_id}',
                    self._recorded_program,
                    self._stream_quality.quality,
                    self._stream_quality.encoding_options,
                    offline_session = self,
                )
                await self._stream.encodeOffline()
            if self._close_task is not None:
                return
            self._is_complete = True
            self._changed.set()
        except Exception as ex:
            logging.error(f'[OfflineVideoSession] Encoding failed. [video_id: {self.info.video_id}]', exc_info=ex)
            self.close()

    async def _cleanup(self) -> None:
        try:
            if self._encoding_task is not None and not self._encoding_task.done():
                if self._stream is not None:
                    self._stream.cancelOfflineEncoding()
                # 入力位置の探索や DB の待機も中断し、エンコード側の finally で資源を回収する。
                self._encoding_task.cancel()
                await asyncio.gather(self._encoding_task, return_exceptions=True)
            while self._sending:
                self._changed.clear()
                await self._changed.wait()
            if self._stream is not None:
                await self._stream.destroy()
        finally:
            self._retained.clear()
            self._retained_bytes = 0
            self._stream = None
            self._sessions.pop(self.session_id)


class OfflineVideoSessionResponse(StreamingResponse):
    """接続単位で応答を中断し、エンコードと lease をセッション側に残す。"""

    def __init__(self, session: OfflineVideoSession, connection_id: int, next_sequence: int) -> None:
        self._session = session
        self._connection_id = connection_id
        self._next_sequence = next_sequence
        super().__init__(
            (),
            media_type = 'application/octet-stream',
            headers = {'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'},
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        is_started = False
        is_finished = False

        async def TrackResponse(message: Message) -> None:
            nonlocal is_started, is_finished
            # send() が通信待ちに入る前に、応答開始を記録する。
            if message['type'] == 'http.response.start':
                is_started = True
            await send(message)
            if message['type'] == 'http.response.body' and not message.get('more_body', False):
                is_finished = True

        with CancelScope() as response_scope:
            if not self._session.bindResponse(self._connection_id, response_scope):
                if self._session.is_closed:
                    error = OfflineVideoSessionError(410, 'session_unavailable')
                else:
                    error = OfflineVideoSessionError(409, 'stale_connection')
                await JSONResponse({'detail': error.detail}, status_code=error.status_code)(scope, receive, send)
                return
            try:
                await super().__call__(scope, receive, TrackResponse)
            except (OSError, ClientDisconnect):
                # ステータス行や本文の送信が失敗しても、同じエンコーダーで再接続を受け付ける。
                pass
            finally:
                self._session.unbindResponse(self._connection_id, response_scope)

        # 引き継ぎや失敗で中断した場合も、書きかけの HTTP 応答を可能な範囲で閉じる。
        if response_scope.cancel_called and is_started and not is_finished:
            try:
                with move_on_after(1):
                    await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
            except OSError:
                pass

    async def stream_response(self, send: Send) -> None:
        await send({'type': 'http.response.start', 'status': self.status_code, 'headers': self.raw_headers})
        await self._session.stream(self._connection_id, self._next_sequence, send)
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
