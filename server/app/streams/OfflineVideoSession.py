from __future__ import annotations

import asyncio
import secrets
import struct
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from anyio import CancelScope, move_on_after
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.requests import ClientDisconnect
from starlette.types import Message, Receive, Scope, Send

from app import logging
from app.metadata.MetadataAnalyzer import MetadataAnalyzer
from app.models.RecordedProgram import RecordedProgram
from app.models.RecordedVideo import RecordedVideo
from app.schemas import (
    OfflineVideoSessionErrorDetail,
    OfflineVideoSessionInfo,
    OfflineVideoSessionStreamMetadata,
)
from app.streams.StreamEncodingOptions import StreamQualityWithOptions
from app.streams.VideoStream import VideoStream


# オフライン保存はエンコーダーを長時間占有するため、通常視聴分の余裕を残して同時実行数を制限する
# 通常 Fetch と Background Fetch のエンコードは同じ実行枠を使う
OFFLINE_VIDEO_STREAM_SEMAPHORE = asyncio.Semaphore(3)


def OfflineVideoSessionError(status_code: int, code: str, retry_after: int | None = None) -> HTTPException:
    """
    オフライン保存クライアントが判別するエラーを返す。

    Args:
        status_code (int): HTTP ステータスコード
        code (str): クライアントが処理を判別するエラーコード
        retry_after (int | None): 再要求までに待つ秒数

    Returns:
        HTTPException: エラーコードと、指定された場合は待機秒数を含む HTTP 例外
    """
    return HTTPException(
        status_code,
        detail = OfflineVideoSessionErrorDetail(code=code),
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
    # エンコード中の入力フィードとは別に、同数の録画照合を並行して進められるようにする。
    _file_io_slots: ClassVar[asyncio.Semaphore] = asyncio.Semaphore(MAX_SESSIONS * 2)
    _file_io_executor: ClassVar[ThreadPoolExecutor] = ThreadPoolExecutor(max_workers=MAX_SESSIONS * 2)
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

        Returns:
            None
        """
        # create() が登録に使い、_cleanup() が回収時に取り除く保存 ID
        self.session_id = session_id
        # _matchesRecording() の照合基準となる作成時の録画情報。_encode() が同じ録画を入力に使う
        self._recorded_program = recorded_program
        # _encode() が使用する検証済みの画質と追加オプション
        self._stream_quality = stream_quality
        # _matchesRecording() が最初の GET で記録し、以後の接続と正常終端で比較するファイルの識別情報
        self._file_identity: tuple[int, int, int, int, int] | None = None
        # connect() が並行 GET で共有する録画照合タスク。close() が待機中の照合を中断する
        self._connection_recording_check: asyncio.Task[bool | None] | None = None
        # create() の応答と stream() の先頭に載せる録画情報・画質・世代トークン
        self.info = OfflineVideoSessionInfo(
            video_id = recorded_program.id,
            file_hash = recorded_program.recorded_video.file_hash,
            quality = quality,
            duration_seconds = recorded_program.recorded_video.duration,
            generation_token = secrets.token_hex(16),
        )

        # publishSegment() が確定した TS と再生時間。stream() は再送時も同じ本体を参照する
        self._retained: dict[int, tuple[bytes, float]] = {}
        # stream() が保持する送信中の参照数。_acknowledge() はこの参照の解放を待ってデータを破棄する
        self._sending: dict[int, int] = {}
        # waitForCapacity() が確認する確定データの総容量。ACK 後も送信中の本体を含む
        self._retained_bytes = 0
        # _acknowledge() が確定した連続範囲の次の番号。connect() が再開位置の下限に使う
        self._acked_next = 0
        # stream() が送信処理へ渡した連続範囲の次の番号。_acknowledge() が確定位置の上限に使う
        self._offered_next = 0
        # publishSegment() が確定した連続範囲の次の番号。stream() が正常終端で件数として返す
        self._produced_next = 0
        # stream() と waitForCapacity() が、生成・容量解放・変換終了を待つための通知
        self._changed = asyncio.Event()
        # _encode() が正常に終了したことを stream() に伝えるフラグ
        self._is_complete = False

        # connect() が受理した最新の接続番号。_validateConnection() の検証に使うため切断後も保持する
        self._connection_id = 0
        # bindResponse() が登録した現応答の中断先。connect() の引き継ぎと close() で使う
        self._response_scope: CancelScope | None = None
        # _encode() が実行枠を取得した後に作成する変換用ストリーム。_cleanup() が破棄する
        self._stream: VideoStream | None = None
        # bindResponse() が初回接続で起動する変換タスク。_cleanup() が終了を待つ
        self._encoding_task: asyncio.Task[None] | None = None
        # close() が一度だけ起動する回収タスク。is_closed はこのタスクの有無で回収開始を判定する
        self._close_task: asyncio.Task[None] | None = None
        # _renewLease() が設定する単調時計上の期限。is_closed はタイマー発火前もこの値を確認する
        self._lease_deadline = 0.0
        # _renewLease() が設定する期限切れタイマー。close() が回収開始時に解除する
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
        """
        録画情報と画質を確認し、同じ保存 ID の並行作成を一つの登録にまとめる。

        Args:
            session_id (str): クライアントが指定した保存 ID
            recorded_program (RecordedProgram): 保存する録画番組
            quality (str): 追加オプションを含む要求画質
            stream_quality (StreamQualityWithOptions): エンコーダーへ渡す画質

        Returns:
            tuple[OfflineVideoSession, bool]: 対象のセッションと、新規登録したかどうか
        """
        recorded_video = recorded_program.recorded_video
        if recorded_video.status != 'Recorded':
            raise OfflineVideoSessionError(409, 'recording_unavailable')

        # ファイル I/O や await を挟まず、上限確認と登録を一度に行う。
        existing = cls._sessions.get(session_id)
        if existing is not None:
            existing._validateTarget(recorded_program.id, quality)
            if existing._recordingFields(recorded_video) != existing._recordingFields(existing._recorded_program.recorded_video):
                raise OfflineVideoSessionError(409, 'recording_changed')
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
        """
        登録済みの対象と世代を照合する。

        Args:
            session_id (str): クライアントが指定した保存 ID
            video_id (int): 保存する録画番組の ID
            quality (str): 追加オプションを含む要求画質
            generation_token (str | None): セッションの作成時に発行した世代トークン

        Returns:
            OfflineVideoSession | None: 対象のセッション。未登録の場合は None
        """
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
        """
        有効期限を確認し、期限切れなら回収を開始する。

        Args:
            なし

        Returns:
            bool: セッションの回収が始まっていれば True
        """
        if self._close_task is None and asyncio.get_running_loop().time() >= self._lease_deadline:
            self.close()
        return self._close_task is not None

    @classmethod
    async def closeAll(cls) -> None:
        """
        サーバー終了時に全セッションの回収を待つ。

        Args:
            なし

        Returns:
            None
        """
        await asyncio.gather(*(session.close() for session in tuple(cls._sessions.values())))

    def close(self) -> asyncio.Task[None]:
        """
        接続を中断し、セッションの回収を一度だけ開始する。

        Args:
            なし

        Returns:
            asyncio.Task[None]: 資源の回収を待つためのタスク
        """
        if self._close_task is None:
            if self._lease_timer is not None:
                self._lease_timer.cancel()
            if self._connection_recording_check is not None:
                self._connection_recording_check.cancel()
            if self._response_scope is not None:
                self._response_scope.cancel()
            self._changed.set()
            self._close_task = asyncio.create_task(self._cleanup())
        return self._close_task

    async def connect(self, connection_id: int, next_sequence: int) -> None:
        """
        同一世代へ接続し、進捗を確定して旧 HTTP 応答を中断する。

        Args:
            connection_id (int): 新しい HTTP 接続の番号
            next_sequence (int): 端末でまだ保存していない最初のセグメント番号

        Returns:
            None
        """
        self._validateConnection(connection_id, is_new=True)

        # 録画ファイルの応答が遅い間に GET が重なっても、進行中の照合を使う。
        if self._connection_recording_check is None or self._connection_recording_check.done():
            self._connection_recording_check = self._startRecordingCheck()
        recording_check = self._connection_recording_check

        # GET 自体の中断からは照合を独立させ、セッション終了時は待機中の GET に 410 を返す。
        await asyncio.wait({recording_check})
        self._validateConnection(connection_id, is_new=True)
        matches_recording = recording_check.result()
        # 一時的な I/O 障害から、同じデータを再取得できるようにセッションを保持する。
        if matches_recording is None:
            raise OfflineVideoSessionError(503, 'recording_io_error')
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
        """
        現接続からの保存位置を累積確認し、セッションの有効期限を更新する。

        Args:
            connection_id (int): 保存位置を通知した HTTP 接続の番号
            next_sequence (int): 端末でまだ保存していない最初のセグメント番号

        Returns:
            int: サーバーが確認済みの位置の次のセグメント番号
        """
        self._validateConnection(connection_id, is_new=False)
        self._acknowledge(next_sequence)
        return self._acked_next

    @classmethod
    async def runFileIO[T, *Args](cls, operation: Callable[[*Args], T], *args: *Args) -> T:
        """
        RAM 保存の録画 I/O を専用プールで実行し、ワーカーが終了するまで実行数に含める。

        Args:
            operation (Callable[[*Args], T]): ワーカーで実行する同期処理
            *args (*Args): 同期処理へ渡す引数

        Returns:
            T: 同期処理の戻り値
        """

        slots = cls._file_io_slots
        await slots.acquire()
        try:
            worker = asyncio.get_running_loop().run_in_executor(cls._file_io_executor, operation, *args)
        except BaseException:
            slots.release()
            raise

        def ReleaseFileIOSlot(done: asyncio.Future[T]) -> None:
            """
            ワーカーの終了後に実行枠と Future の例外を回収する。

            Args:
                done (asyncio.Future[T]): 終了した録画 I/O の Future

            Returns:
                None
            """
            slots.release()
            # 呼び出し元が離脱した場合も、完了した Future の例外を回収する。
            if not done.cancelled():
                done.exception()

        worker.add_done_callback(ReleaseFileIOSlot)
        return await asyncio.shield(worker)

    async def waitForCapacity(self, encoder: asyncio.subprocess.Process) -> bool:
        """
        未確認データに最大サイズのセグメントを一つ追加できるまで待つ。

        Args:
            encoder (asyncio.subprocess.Process): 待機中に終了コードを確認するエンコーダー

        Returns:
            bool: 容量を確保できた場合は True。セッションの回収が始まった場合は False
        """
        while self._close_task is None:
            # 空きがあれば stdout 側で起動失敗を判定し、既存の起動リトライへ進める。
            if self._retained_bytes + self.MAX_SEGMENT_BYTES <= self.RETAINED_BYTES_LIMIT:
                return True
            if encoder.returncode not in (None, 0):
                raise RuntimeError('Encoder exited while waiting for offline segment capacity.')
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

        Returns:
            None
        """
        assert sequence == self._produced_next
        assert 0 < len(data) <= self.MAX_SEGMENT_BYTES
        assert self._retained_bytes + len(data) <= self.RETAINED_BYTES_LIMIT
        self._retained[sequence] = (bytes(data), duration_seconds)
        self._retained_bytes += len(data)
        self._produced_next += 1
        self._changed.set()

    async def stream(self, connection_id: int, next_sequence: int, send: Send) -> None:
        """
        現接続のメタデータ、セグメント、終端を順に送る。

        Args:
            connection_id (int): 現在の HTTP 接続の番号
            next_sequence (int): 送信を開始するセグメント番号
            send (Send): HTTP レスポンスの本文を送る ASGI 関数

        Returns:
            None
        """
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
                recording_check = self._startRecordingCheck()
                try:
                    # 終端前の DB・ファイル照合が遅くても、受信側の無通信タイムアウトを発生させない。
                    while True:
                        done, _ = await asyncio.wait({recording_check}, timeout=keepalive_deadline - loop.time())
                        if self._close_task is not None or connection_id != self._connection_id:
                            return
                        if done:
                            break
                        await send({
                            'type': 'http.response.body',
                            'body': struct.pack('>II', 0xfffffffe, 0),
                            'more_body': True,
                        })
                        keepalive_deadline = loop.time() + self.KEEPALIVE_INTERVAL
                    matches_recording = recording_check.result()
                    # 一時的なエラーでは終端を省き、次の GET で再照合する。
                    if matches_recording is None:
                        return
                    if matches_recording:
                        await send({
                            'type': 'http.response.body',
                            'body': struct.pack('>II', 0xffffffff, sequence),
                            'more_body': True,
                        })
                    else:
                        self.close()
                finally:
                    recording_check.cancel()
                    await asyncio.gather(recording_check, return_exceptions=True)
                return

            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=keepalive_deadline - loop.time())
            except TimeoutError:
                if self._close_task is None and connection_id == self._connection_id:
                    await send({
                        'type': 'http.response.body',
                        'body': struct.pack('>II', 0xfffffffe, 0),
                        'more_body': True,
                    })
                keepalive_deadline = loop.time() + self.KEEPALIVE_INTERVAL

    def bindResponse(self, connection_id: int, scope: CancelScope) -> bool:
        """
        現接続のレスポンスを中断できるように登録し、初回はエンコードを開始する。

        Args:
            connection_id (int): レスポンスを開始する HTTP 接続の番号
            scope (CancelScope): 現接続を中断するためのスコープ

        Returns:
            bool: 現接続としてレスポンスを開始できる場合は True
        """
        if self.is_closed or self._connection_id != connection_id:
            return False
        self._response_scope = scope
        if self._encoding_task is None:
            self._encoding_task = asyncio.create_task(self._encode())
        return True

    def unbindResponse(self, connection_id: int, scope: CancelScope) -> None:
        """
        終了したレスポンスに対応する中断スコープを外す。

        Args:
            connection_id (int): レスポンスを終了した HTTP 接続の番号
            scope (CancelScope): 終了したレスポンスのスコープ

        Returns:
            None
        """
        if self._connection_id == connection_id and self._response_scope is scope:
            self._response_scope = None

    def _validateTarget(self, video_id: int, quality: str) -> None:
        """
        要求した録画と画質が、登録されたセッションと一致することを確認する。

        Args:
            video_id (int): 要求された録画番組の ID
            quality (str): 追加オプションを含む要求画質

        Returns:
            None
        """
        if self.info.video_id != video_id or self.info.quality != quality:
            raise OfflineVideoSessionError(409, 'session_mismatch')

    def _validateConnection(self, connection_id: int, *, is_new: bool) -> None:
        """
        有効なセッションに対する、新規接続または現接続からの要求であることを確認する。

        Args:
            connection_id (int): 要求元の接続番号
            is_new (bool): 接続を引き継ぐ GET の場合は True

        Returns:
            None
        """
        if self.is_closed:
            raise OfflineVideoSessionError(410, 'session_unavailable')
        if is_new and connection_id <= self._connection_id:
            raise OfflineVideoSessionError(409, 'stale_connection')
        if not is_new and (self._connection_id == 0 or connection_id != self._connection_id):
            raise OfflineVideoSessionError(409, 'stale_connection')

    def _acknowledge(self, next_sequence: int) -> None:
        """
        検証済みの接続から保存確定位置を受け取り、保持データと有効期限を更新する。

        Args:
            next_sequence (int): 端末でまだ保存していない最初のセグメント番号

        Returns:
            None
        """

        # 送信処理へ渡した範囲までを確定し、送信中の参照は送信側の終了処理で解放する。
        if next_sequence > self._offered_next:
            raise OfflineVideoSessionError(409, 'invalid_checkpoint')
        for sequence in range(self._acked_next, next_sequence):
            if sequence not in self._sending:
                self._releaseSegment(sequence)
        self._acked_next = max(self._acked_next, next_sequence)
        self._changed.set()
        self._renewLease()

    def _renewLease(self) -> None:
        """
        セッションの有効期限を現在時刻から更新し、回収タイマーを設定する。

        Args:
            なし

        Returns:
            None
        """
        if self._lease_timer is not None:
            self._lease_timer.cancel()
        loop = asyncio.get_running_loop()
        self._lease_deadline = loop.time() + self.LEASE_TIMEOUT
        self._lease_timer = loop.call_at(self._lease_deadline, self.close)

    def _releaseSegment(self, sequence: int) -> None:
        """
        保存確認済みで送信中の参照がないセグメントを解放する。

        Args:
            sequence (int): 解放するセグメントの連続番号

        Returns:
            None
        """
        data, _ = self._retained.pop(sequence)
        self._retained_bytes -= len(data)

    @staticmethod
    def _recordingFields(video: RecordedVideo) -> tuple[int, str, str, int, datetime]:
        """
        セッションの作成時から変化していないことを確認する録画の属性を取り出す。

        Args:
            video (RecordedVideo): 照合対象の録画ファイルの DB レコード

        Returns:
            tuple[int, str, str, int, datetime]: 録画ファイルの ID・パス・ハッシュ・サイズ・更新日時
        """
        return video.id, video.file_path, video.file_hash, video.file_size, video.file_modified_at

    def _startRecordingCheck(self) -> asyncio.Task[bool | None]:
        """
        録画照合を開始し、想定外の例外が起きた場合は保存セッションを終了する。

        Args:
            なし

        Returns:
            asyncio.Task[bool | None]: 録画照合の結果を返すタスク
        """

        def FinishRecordingCheck(task: asyncio.Task[bool | None]) -> None:
            """
            GET が既に切断されていても、照合中の実装上の失敗を記録してセッションを回収する。

            Args:
                task (asyncio.Task[bool | None]): 終了した録画照合のタスク

            Returns:
                None
            """
            if task.cancelled():
                return
            if (error := task.exception()) is not None:
                logging.error(f'[OfflineVideoSession] Recording verification failed. [video_id: {self.info.video_id}]', exc_info=error)
                self.close()

        task = asyncio.create_task(self._matchesRecording())
        task.add_done_callback(FinishRecordingCheck)
        return task

    async def _matchesRecording(self) -> bool | None:
        """
        現在の録画情報と実ファイルをセッション作成時の録画と照合する。

        Args:
            なし

        Returns:
            bool | None: 録画が一致すれば True、変更されていれば False、一時的な I/O 障害なら None
        """
        recorded_program = await RecordedProgram.filter(id=self.info.video_id).get_or_none().select_related('recorded_video')
        if recorded_program is None or recorded_program.recorded_video.status != 'Recorded':
            return False
        video = recorded_program.recorded_video
        if self._recordingFields(video) != self._recordingFields(self._recorded_program.recorded_video):
            return False
        verify_hash = self._file_identity is None

        def GetFileIdentity() -> tuple[int, int, int, int, int] | None:
            """
            DB に登録された録画に対応する実ファイルの識別情報を取得する。

            Args:
                なし

            Returns:
                tuple[int, int, int, int, int] | None: stat() で取得したファイルの識別情報。欠損や不一致の場合は None
            """
            path = Path(video.file_path)
            try:
                before = path.stat()
                if before.st_size != video.file_size:
                    return None
                identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                # 同じ内容なら録画スキャンは DB の更新日時を更新しない。初回はサイズとハッシュを照合し、
                # 以後の更新日時の変化はセッションが保持する実ファイルの identity で検出する。
                if verify_hash:
                    if not MetadataAnalyzer(path).matchesFileHash(video.file_hash, raise_on_error=True):
                        return None
                    # 読み取ったハッシュと記録する識別情報を同じファイルのものにそろえる。
                    after = path.stat()
                    if identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        return None
                return identity
            except FileNotFoundError:
                return None

        # 照合中に GET が中断された場合も、想定内のファイル I/O 障害は照合タスク内で処理する。
        try:
            file_identity = await self.runFileIO(GetFileIdentity)
        except OSError as ex:
            logging.warning(f'[OfflineVideoSession] Failed to verify recording. [video_id: {self.info.video_id}]', exc_info=ex)
            return None
        if file_identity is None:
            return False
        if self._file_identity is None:
            self._file_identity = file_identity
        return self._file_identity == file_identity

    async def _encode(self) -> None:
        """
        実行枠を取得し、先頭からエンコードして完了または失敗を通知する。

        Args:
            なし

        Returns:
            None
        """
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
        """
        エンコーダーと送信中の参照の終了を待ち、保持データとセッション登録を回収する。

        Args:
            なし

        Returns:
            None
        """
        try:
            if self._encoding_task is not None and not self._encoding_task.done():
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
        """
        Args:
            session (OfflineVideoSession): 接続と配信データを所有する RAM 保存セッション
            connection_id (int): connect() が受理した HTTP 接続の番号
            next_sequence (int): 端末が指定した再送開始位置

        Returns:
            None
        """
        # 応答の登録と本文の送信を担当する RAM 保存セッション
        self._session = session
        # connect() が受理し、応答登録時に照合する接続番号
        self._connection_id = connection_id
        # stream_response() が本文の送信を開始する位置
        self._next_sequence = next_sequence
        super().__init__(
            (),
            media_type = 'application/octet-stream',
            headers = {'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'},
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        セッションへの接続を登録し、引き継ぎや終了時に HTTP 応答を閉じる。

        Args:
            scope (Scope): HTTP リクエストの ASGI スコープ
            receive (Receive): ASGI の受信関数
            send (Send): ASGI の送信関数

        Returns:
            None
        """
        is_response_open = False

        async def TrackResponse(message: Message) -> None:
            """
            HTTP 応答の開始と終了を記録しながら ASGI メッセージを送る。

            Args:
                message (Message): 送信する ASGI メッセージ

            Returns:
                None
            """
            nonlocal is_response_open
            # send() が通信待ちに入る前に、応答開始を記録する。
            if message['type'] == 'http.response.start':
                is_response_open = True
            await send(message)
            if message['type'] == 'http.response.body' and not message.get('more_body', False):
                is_response_open = False

        with CancelScope() as response_scope:
            # 応答開始前の引き継ぎや期限切れは、通常の API 例外ハンドラーに返す
            if not self._session.bindResponse(self._connection_id, response_scope):
                if self._session.is_closed:
                    raise OfflineVideoSessionError(410, 'session_unavailable')
                raise OfflineVideoSessionError(409, 'stale_connection')
            try:
                await super().__call__(scope, receive, TrackResponse)
            except (OSError, ClientDisconnect):
                # ステータス行や本文の送信が失敗しても、同じエンコーダーで再接続を受け付ける。
                pass
            finally:
                self._session.unbindResponse(self._connection_id, response_scope)

        # 引き継ぎや失敗で中断した場合も、書きかけの HTTP 応答を可能な範囲で閉じる。
        if response_scope.cancel_called and is_response_open:
            try:
                with move_on_after(1):
                    await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
            except OSError:
                pass

    async def stream_response(self, send: Send) -> None:
        """
        セッションから取得したデータを HTTP 応答の本文として送る。

        Args:
            send (Send): ASGI の送信関数

        Returns:
            None
        """
        await send({'type': 'http.response.start', 'status': self.status_code, 'headers': self.raw_headers})
        await self._session.stream(self._connection_id, self._next_sequence, send)
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})
