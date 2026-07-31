from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MemoryBlob(_message.Message):
    __slots__ = ("state_schema_version", "entries", "total_value_bytes")
    class MemoryEntry(_message.Message):
        __slots__ = ("key", "value", "last_access_ms")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        LAST_ACCESS_MS_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        last_access_ms: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., last_access_ms: _Optional[int] = ...) -> None: ...
    STATE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_VALUE_BYTES_FIELD_NUMBER: _ClassVar[int]
    state_schema_version: int
    entries: _containers.RepeatedCompositeFieldContainer[MemoryBlob.MemoryEntry]
    total_value_bytes: int
    def __init__(self, state_schema_version: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[MemoryBlob.MemoryEntry, _Mapping]]] = ..., total_value_bytes: _Optional[int] = ...) -> None: ...

class ToolIntent(_message.Message):
    __slots__ = ("intent_id", "entity_key", "seq", "step_index", "tool_name", "args_json", "created_at_ms", "expires_at_ms", "attempt", "kind", "trace_id", "signature_scheme", "signing_key_id", "signature")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TOOL_KIND_UNSPECIFIED: _ClassVar[ToolIntent.Kind]
        TOOL: _ClassVar[ToolIntent.Kind]
        APPROVAL: _ClassVar[ToolIntent.Kind]
    TOOL_KIND_UNSPECIFIED: ToolIntent.Kind
    TOOL: ToolIntent.Kind
    APPROVAL: ToolIntent.Kind
    class SignatureScheme(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SIGNATURE_SCHEME_UNSPECIFIED: _ClassVar[ToolIntent.SignatureScheme]
        HMAC_SHA256: _ClassVar[ToolIntent.SignatureScheme]
    SIGNATURE_SCHEME_UNSPECIFIED: ToolIntent.SignatureScheme
    HMAC_SHA256: ToolIntent.SignatureScheme
    INTENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STEP_INDEX_FIELD_NUMBER: _ClassVar[int]
    TOOL_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_JSON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_SCHEME_FIELD_NUMBER: _ClassVar[int]
    SIGNING_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    intent_id: str
    entity_key: bytes
    seq: int
    step_index: int
    tool_name: str
    args_json: str
    created_at_ms: int
    expires_at_ms: int
    attempt: int
    kind: ToolIntent.Kind
    trace_id: bytes
    signature_scheme: ToolIntent.SignatureScheme
    signing_key_id: str
    signature: bytes
    def __init__(self, intent_id: _Optional[str] = ..., entity_key: _Optional[bytes] = ..., seq: _Optional[int] = ..., step_index: _Optional[int] = ..., tool_name: _Optional[str] = ..., args_json: _Optional[str] = ..., created_at_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., attempt: _Optional[int] = ..., kind: _Optional[_Union[ToolIntent.Kind, str]] = ..., trace_id: _Optional[bytes] = ..., signature_scheme: _Optional[_Union[ToolIntent.SignatureScheme, str]] = ..., signing_key_id: _Optional[str] = ..., signature: _Optional[bytes] = ...) -> None: ...

class ToolResult(_message.Message):
    __slots__ = ("intent_id", "entity_key", "seq", "status", "payload", "error_message", "completed_at_ms")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[ToolResult.Status]
        OK: _ClassVar[ToolResult.Status]
        ERROR: _ClassVar[ToolResult.Status]
        EXPIRED: _ClassVar[ToolResult.Status]
        REJECTED: _ClassVar[ToolResult.Status]
    STATUS_UNSPECIFIED: ToolResult.Status
    OK: ToolResult.Status
    ERROR: ToolResult.Status
    EXPIRED: ToolResult.Status
    REJECTED: ToolResult.Status
    INTENT_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    intent_id: str
    entity_key: bytes
    seq: int
    status: ToolResult.Status
    payload: bytes
    error_message: str
    completed_at_ms: int
    def __init__(self, intent_id: _Optional[str] = ..., entity_key: _Optional[bytes] = ..., seq: _Optional[int] = ..., status: _Optional[_Union[ToolResult.Status, str]] = ..., payload: _Optional[bytes] = ..., error_message: _Optional[str] = ..., completed_at_ms: _Optional[int] = ...) -> None: ...

class TraceEvent(_message.Message):
    __slots__ = ("trace_id", "span_id", "parent_span_id", "entity_key", "seq", "step_index", "event_type", "attributes", "start_ms", "end_ms")
    class EventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        EVENT_TYPE_UNSPECIFIED: _ClassVar[TraceEvent.EventType]
        ACTIVATION_START: _ClassVar[TraceEvent.EventType]
        LLM_CALL: _ClassVar[TraceEvent.EventType]
        TOOL_CALL: _ClassVar[TraceEvent.EventType]
        INTENT_EMITTED: _ClassVar[TraceEvent.EventType]
        ACTIVATION_END: _ClassVar[TraceEvent.EventType]
        ERROR: _ClassVar[TraceEvent.EventType]
        SUSPENDED: _ClassVar[TraceEvent.EventType]
    EVENT_TYPE_UNSPECIFIED: TraceEvent.EventType
    ACTIVATION_START: TraceEvent.EventType
    LLM_CALL: TraceEvent.EventType
    TOOL_CALL: TraceEvent.EventType
    INTENT_EMITTED: TraceEvent.EventType
    ACTIVATION_END: TraceEvent.EventType
    ERROR: TraceEvent.EventType
    SUSPENDED: TraceEvent.EventType
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STEP_INDEX_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    START_MS_FIELD_NUMBER: _ClassVar[int]
    END_MS_FIELD_NUMBER: _ClassVar[int]
    trace_id: bytes
    span_id: bytes
    parent_span_id: bytes
    entity_key: bytes
    seq: int
    step_index: int
    event_type: TraceEvent.EventType
    attributes: _containers.ScalarMap[str, str]
    start_ms: int
    end_ms: int
    def __init__(self, trace_id: _Optional[bytes] = ..., span_id: _Optional[bytes] = ..., parent_span_id: _Optional[bytes] = ..., entity_key: _Optional[bytes] = ..., seq: _Optional[int] = ..., step_index: _Optional[int] = ..., event_type: _Optional[_Union[TraceEvent.EventType, str]] = ..., attributes: _Optional[_Mapping[str, str]] = ..., start_ms: _Optional[int] = ..., end_ms: _Optional[int] = ...) -> None: ...

class AgentEnvelope(_message.Message):
    __slots__ = ("entity_key", "event_time_ms", "external_event", "tool_result", "approval", "export_request")
    class Approval(_message.Message):
        __slots__ = ("intent_id", "approved", "approver", "decided_at_ms")
        INTENT_ID_FIELD_NUMBER: _ClassVar[int]
        APPROVED_FIELD_NUMBER: _ClassVar[int]
        APPROVER_FIELD_NUMBER: _ClassVar[int]
        DECIDED_AT_MS_FIELD_NUMBER: _ClassVar[int]
        intent_id: str
        approved: bool
        approver: str
        decided_at_ms: int
        def __init__(self, intent_id: _Optional[str] = ..., approved: _Optional[bool] = ..., approver: _Optional[str] = ..., decided_at_ms: _Optional[int] = ...) -> None: ...
    class StateExportRequest(_message.Message):
        __slots__ = ("request_id",)
        REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
        request_id: str
        def __init__(self, request_id: _Optional[str] = ...) -> None: ...
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_EVENT_FIELD_NUMBER: _ClassVar[int]
    TOOL_RESULT_FIELD_NUMBER: _ClassVar[int]
    APPROVAL_FIELD_NUMBER: _ClassVar[int]
    EXPORT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    entity_key: bytes
    event_time_ms: int
    external_event: bytes
    tool_result: ToolResult
    approval: AgentEnvelope.Approval
    export_request: AgentEnvelope.StateExportRequest
    def __init__(self, entity_key: _Optional[bytes] = ..., event_time_ms: _Optional[int] = ..., external_event: _Optional[bytes] = ..., tool_result: _Optional[_Union[ToolResult, _Mapping]] = ..., approval: _Optional[_Union[AgentEnvelope.Approval, _Mapping]] = ..., export_request: _Optional[_Union[AgentEnvelope.StateExportRequest, _Mapping]] = ...) -> None: ...

class StateSnapshot(_message.Message):
    __slots__ = ("state_schema_version", "entity_key", "seq", "snapshot_at_ms", "memory", "llm_cache", "continuation", "pending", "request_id")
    STATE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_FIELD_NUMBER: _ClassVar[int]
    LLM_CACHE_FIELD_NUMBER: _ClassVar[int]
    CONTINUATION_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    state_schema_version: int
    entity_key: bytes
    seq: int
    snapshot_at_ms: int
    memory: MemoryBlob
    llm_cache: LlmCacheBlob
    continuation: Continuation
    pending: _containers.RepeatedCompositeFieldContainer[ToolIntent]
    request_id: str
    def __init__(self, state_schema_version: _Optional[int] = ..., entity_key: _Optional[bytes] = ..., seq: _Optional[int] = ..., snapshot_at_ms: _Optional[int] = ..., memory: _Optional[_Union[MemoryBlob, _Mapping]] = ..., llm_cache: _Optional[_Union[LlmCacheBlob, _Mapping]] = ..., continuation: _Optional[_Union[Continuation, _Mapping]] = ..., pending: _Optional[_Iterable[_Union[ToolIntent, _Mapping]]] = ..., request_id: _Optional[str] = ...) -> None: ...

class ActivationErrorRecord(_message.Message):
    __slots__ = ("entity_key", "reason", "detail", "event_time_ms")
    ENTITY_KEY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    entity_key: bytes
    reason: str
    detail: str
    event_time_ms: int
    def __init__(self, entity_key: _Optional[bytes] = ..., reason: _Optional[str] = ..., detail: _Optional[str] = ..., event_time_ms: _Optional[int] = ...) -> None: ...

class Continuation(_message.Message):
    __slots__ = ("state_schema_version", "seq", "step_index", "pending_intent_ids", "adapter", "snapshot", "suspended_at_ms", "deadline_ms", "escalations")
    STATE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    STEP_INDEX_FIELD_NUMBER: _ClassVar[int]
    PENDING_INTENT_IDS_FIELD_NUMBER: _ClassVar[int]
    ADAPTER_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    SUSPENDED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    ESCALATIONS_FIELD_NUMBER: _ClassVar[int]
    state_schema_version: int
    seq: int
    step_index: int
    pending_intent_ids: _containers.RepeatedScalarFieldContainer[str]
    adapter: str
    snapshot: bytes
    suspended_at_ms: int
    deadline_ms: int
    escalations: int
    def __init__(self, state_schema_version: _Optional[int] = ..., seq: _Optional[int] = ..., step_index: _Optional[int] = ..., pending_intent_ids: _Optional[_Iterable[str]] = ..., adapter: _Optional[str] = ..., snapshot: _Optional[bytes] = ..., suspended_at_ms: _Optional[int] = ..., deadline_ms: _Optional[int] = ..., escalations: _Optional[int] = ...) -> None: ...

class LlmCacheBlob(_message.Message):
    __slots__ = ("state_schema_version", "entries", "total_response_bytes")
    class LlmCacheEntry(_message.Message):
        __slots__ = ("cache_key", "response", "response_digest", "created_at_ms", "last_access_ms", "digest_only")
        CACHE_KEY_FIELD_NUMBER: _ClassVar[int]
        RESPONSE_FIELD_NUMBER: _ClassVar[int]
        RESPONSE_DIGEST_FIELD_NUMBER: _ClassVar[int]
        CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
        LAST_ACCESS_MS_FIELD_NUMBER: _ClassVar[int]
        DIGEST_ONLY_FIELD_NUMBER: _ClassVar[int]
        cache_key: str
        response: bytes
        response_digest: bytes
        created_at_ms: int
        last_access_ms: int
        digest_only: bool
        def __init__(self, cache_key: _Optional[str] = ..., response: _Optional[bytes] = ..., response_digest: _Optional[bytes] = ..., created_at_ms: _Optional[int] = ..., last_access_ms: _Optional[int] = ..., digest_only: _Optional[bool] = ...) -> None: ...
    STATE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_RESPONSE_BYTES_FIELD_NUMBER: _ClassVar[int]
    state_schema_version: int
    entries: _containers.RepeatedCompositeFieldContainer[LlmCacheBlob.LlmCacheEntry]
    total_response_bytes: int
    def __init__(self, state_schema_version: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[LlmCacheBlob.LlmCacheEntry, _Mapping]]] = ..., total_response_bytes: _Optional[int] = ...) -> None: ...

class LongTermRecord(_message.Message):
    __slots__ = ("state_schema_version", "key", "value", "seq", "updated_at_ms")
    STATE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    state_schema_version: int
    key: str
    value: bytes
    seq: int
    updated_at_ms: int
    def __init__(self, state_schema_version: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[bytes] = ..., seq: _Optional[int] = ..., updated_at_ms: _Optional[int] = ...) -> None: ...
