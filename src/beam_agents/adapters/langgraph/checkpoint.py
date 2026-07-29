"""`BeamCheckpointSaver`: LangGraph checkpoints in the activation's working memory.

The saver holds the activation's staged :class:`~beam_agents.memory.facade.Memory`
facade and stores the **latest** checkpoint (channel values — including message
history — plus metadata) and the interrupted superstep's pending writes as two
scalars under the reserved ``__langgraph__/`` key namespace. Because the facade
stages in memory and the stateful DoFn commits the resulting ``MemoryBlob``
atomically with the Beam bundle, checkpoint durability *is* bundle atomicity
(correctness invariant 1): a failed activation leaves no partial checkpoint, and
a worker failover reloads the committed blob and resumes from the last committed
superstep.

Retention is latest-only by design (change design D2): Beam keyed state is the
durability layer, not a checkpoint log, so there is no parent chain and
``list()`` yields at most one tuple. Checkpoint size is bounded by the working
memory hard cap — an oversized checkpoint raises
:class:`~beam_agents.memory.facade.MemoryOverflow`, failing the activation
closed with no partial state. Keep graph state small: trim or summarize message
history on the LangGraph side (e.g. a reducer that drops old messages) well
before the 1 MiB cap.

All saver I/O is against staged in-memory state — no network, no blocking — so
the sync methods are the implementation and the async variants delegate
directly; nothing ever blocks the bridge event loop.

Compactors must not evict ``__langgraph__/`` keys: they are load-bearing resume
state, not cache.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.serde.base import SerializerProtocol

    from beam_agents.memory.facade import Memory

# The reserved working-memory namespace for LangGraph state. The adapter owns
# every key under this prefix; nothing else may write here.
RESERVED_NAMESPACE = "__langgraph__/"
_CKPT_KEY = RESERVED_NAMESPACE + "ckpt"
_WRITES_KEY = RESERVED_NAMESPACE + "writes"

# Frame a (serde type tag, payload) pair into one scalar value: u16 big-endian
# tag length + tag + payload. The tag is serde-defined ("msgpack", "json", ...)
# and must survive round-trips for `loads_typed` to pick the right decoder.
_TAG_LEN_PREFIX = 2

# One stored pending-write row: (task_id, idx, channel, tagged value, task_path,
# checkpoint_id). Aliased at module scope because `list[...]` inside the class
# body would resolve to the `list()` *method* the checkpointer ABC mandates.
_WriteRow = tuple[str, int, str, tuple[str, bytes], str, str]
_WriteRows = list[_WriteRow]
_ScopedWriteRows = list[tuple[str, int, str, tuple[str, bytes], str]]


def _frame(tagged: tuple[str, bytes]) -> bytes:
    tag, payload = tagged
    tag_bytes = tag.encode("utf-8")
    return len(tag_bytes).to_bytes(_TAG_LEN_PREFIX, "big") + tag_bytes + payload


def _unframe(value: bytes) -> tuple[str, bytes]:
    tag_len = int.from_bytes(value[:_TAG_LEN_PREFIX], "big")
    tag_end = _TAG_LEN_PREFIX + tag_len
    return value[_TAG_LEN_PREFIX:tag_end].decode("utf-8"), value[tag_end:]


class BeamCheckpointSaver(BaseCheckpointSaver[str]):
    """Latest-only checkpoint persistence over one activation's working memory."""

    def __init__(self, memory: Memory, *, serde: SerializerProtocol | None = None) -> None:
        super().__init__(serde=serde if serde is not None else JsonPlusSerializer())
        self._memory = memory

    # -- sync implementation ---------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        raw = self._memory.get(_CKPT_KEY)
        if raw is None:
            return None
        envelope = self._loads(raw)
        wanted = config["configurable"].get("checkpoint_id")
        if wanted and wanted != envelope["id"]:
            # Latest-only: a request for a superseded checkpoint has nothing to
            # return — there is no history to serve it from.
            return None
        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": config["configurable"]["thread_id"],
                    "checkpoint_ns": envelope["ns"],
                    "checkpoint_id": envelope["id"],
                }
            },
            checkpoint=self.serde.loads_typed(tuple(envelope["checkpoint"])),
            metadata=self.serde.loads_typed(tuple(envelope["metadata"])),
            parent_config=None,
            pending_writes=[
                (task_id, channel, self.serde.loads_typed(tagged))
                for task_id, _idx, channel, tagged, _path in self._writes_for(envelope["id"])
            ],
        )

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        # Latest-only: there is at most one tuple, and `before` any checkpoint
        # there is nothing older to return.
        if config is None or before is not None or (limit is not None and limit < 1):
            return
        checkpoint_tuple = self.get_tuple(config)
        if checkpoint_tuple is not None:
            yield checkpoint_tuple

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        envelope = {
            "id": checkpoint["id"],
            "ns": checkpoint_ns,
            "checkpoint": self.serde.dumps_typed(checkpoint),
            "metadata": self.serde.dumps_typed(dict(metadata)),
        }
        self._memory.set(_CKPT_KEY, _frame(self.serde.dumps_typed(envelope)))
        # Latest-only retention: writes recorded against superseded checkpoints
        # can never be served again, so drop them with the checkpoint they
        # belonged to.
        rows = self._all_writes()
        kept = [row for row in rows if row[5] == checkpoint["id"]]
        if len(kept) != len(rows):
            self._store_writes(kept)
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Any,
        task_id: str,
        task_path: str = "",
    ) -> None:
        checkpoint_id = config["configurable"]["checkpoint_id"]
        rows = self._all_writes()
        # Dict semantics on (checkpoint_id, task_id, idx): a re-executed task
        # (an interrupted node re-entered on resume) overwrites its own prior
        # writes instead of duplicating them. Special channels dedupe to fixed
        # indices, mirroring the reference savers.
        by_key = {(row[5], row[0], row[1]): row for row in rows}
        for position, (channel, value) in enumerate(writes):
            idx = WRITES_IDX_MAP.get(channel, position)
            by_key[(checkpoint_id, task_id, idx)] = (
                task_id,
                idx,
                channel,
                self.serde.dumps_typed(value),
                task_path,
                checkpoint_id,
            )
        self._store_writes(list(by_key.values()))

    # -- async delegation (all I/O is staged in memory; nothing blocks) --------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self.get_tuple(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        for checkpoint_tuple in self.list(config, filter=filter, before=before, limit=limit):
            yield checkpoint_tuple

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self.put(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Any,
        task_id: str,
        task_path: str = "",
    ) -> None:
        self.put_writes(config, writes, task_id, task_path)

    # -- internals -------------------------------------------------------------

    def _loads(self, value: bytes) -> dict[str, Any]:
        decoded: dict[str, Any] = self.serde.loads_typed(_unframe(value))
        return decoded

    def _all_writes(self) -> _WriteRows:
        """All stored write rows: (task_id, idx, channel, tagged, path, ckpt_id)."""
        raw = self._memory.get(_WRITES_KEY)
        if raw is None:
            return []
        rows: list[Any] = self._loads(raw)["rows"]
        return [(row[0], row[1], row[2], (row[3][0], row[3][1]), row[4], row[5]) for row in rows]

    def _writes_for(self, checkpoint_id: str) -> _ScopedWriteRows:
        rows = [row for row in self._all_writes() if row[5] == checkpoint_id]
        rows.sort(key=lambda row: (row[0], row[1]))
        return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    def _store_writes(self, rows: _WriteRows) -> None:
        self._memory.set(_WRITES_KEY, _frame(self.serde.dumps_typed({"rows": rows})))
