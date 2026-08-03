/**
 * The runtime's closed error vocabulary, in plain English.
 *
 * Sourced from `docs/errors.md` and the constants it documents
 * (`core/dofn.py:152-186`, `hitl.py`). The whole reason grouping by `reason` is
 * a navigation axis rather than a string histogram is that this list is closed
 * and small — so the UI is allowed to know it, and someone meeting
 * `orphaned_result` or `ttl_wiped_suspension` for the first time should not have
 * to go and read the source to find out what just happened to their pipeline.
 *
 * A reason the console does not recognise is rendered as such rather than
 * guessed at: the vocabulary is closed *for this runtime version*, and an
 * unknown value means the records came from somewhere this build does not know
 * about, which is itself worth saying out loud.
 */

/** One line of prose per reason, plus what it implies for committed state. */
export interface ReasonInfo {
  /** What happened, in one sentence. */
  summary: string;
  /** What survived the failure. Stated per reason, because it genuinely differs. */
  consequence: string;
  /** Where the record comes from, for the reader who wants the source. */
  source: string;
  /** What the `detail` string carries for this reason, when it carries anything. */
  detail: string;
}

export const REASON_INFO: Record<string, ReasonInfo> = {
  activation_error: {
    summary:
      'The agent raised, and the failure was routed to .errors rather than failing the bundle.',
    consequence:
      'Nothing committed: staged intents, memory writes, cache inserts, traces and outputs were all discarded, and seq did not advance.',
    source: 'core/dofn.py · REASON_ERROR',
    detail: "The original exception's repr, then failed_at_step and the last staged event.",
  },
  activation_timeout: {
    summary: 'The activation ran past activation_timeout_s and its coroutine was cancelled.',
    consequence:
      'Nothing committed, and no failure position was captured — the coroutine may still have been running, so there was no context to read.',
    source: 'core/dofn.py · REASON_TIMEOUT',
    detail: 'Empty. There is no exception to name.',
  },
  budget_exceeded: {
    summary:
      'The activation crossed max_tokens_per_activation. This is a cost question, not a stack-trace one.',
    consequence:
      'Nothing committed and seq did not advance. A resume starts a fresh meter — the budget bounds an attempt, not a seq.',
    source: 'core/dofn.py · REASON_BUDGET_EXCEEDED',
    detail: 'BudgetExceeded(limit=…, consumed=…), then the failure position.',
  },
  orphaned_result: {
    summary: 'A tool result or approval came back with no live continuation left to admit it.',
    consequence:
      'No state was mutated: the result was refused, not applied. The suspended activation, if there still is one, is untouched.',
    source: 'core/dofn.py · REASON_ORPHANED',
    detail: 'One of four admission failures, then the intent id — <why>:<intent_id>.',
  },
  hitl_timeout: {
    summary:
      "An approval never arrived and the policy's timeout route dropped it rather than denying.",
    consequence:
      'The suspension ended with nothing on the main output. A Deny route would have emitted bytes instead; this one records the timeout here.',
    source: 'hitl.py · REASON_HITL_TIMEOUT',
    detail: 'Set by the policy that produced the timeout.',
  },
  ttl_wiped_suspension: {
    summary: 'Working-memory GC reached a key that was still awaiting an answer.',
    consequence:
      'The suspension is unrecoverable — nothing can resume it. The TTL timer and the suspension deadline read different clocks, so a backlog replay can cross the event-time mark first.',
    source: 'core/dofn.py · REASON_TTL_WIPED_SUSPENSION',
    detail: 'Empty. The record exists so that the loss is observable at all.',
  },
  ttl_wiped_batch: {
    summary: 'Working-memory GC reached a key whose batching buffer still held un-flushed events.',
    consequence:
      'The buffered envelopes are gone, one record per envelope — so the loss is element-granular and replayable rather than silent.',
    source: 'core/dofn.py · REASON_TTL_WIPED_BATCH',
    detail: 'buffered=<n>,index=<i>.',
  },
  batch_buffer_overflow: {
    summary: 'An event arrived at a key whose batching buffer already held max_buffered_events.',
    consequence:
      'The arriving event was dropped and counted, rather than growing keyed state silently toward the 1 MiB cap.',
    source: 'core/dofn.py · REASON_BATCH_OVERFLOW',
    detail: 'buffered=<n>,cap=<n>.',
  },
  intent_dead_letter: {
    summary: 'An intent could not be serialized for the outbox, so WriteIntents dead-lettered it.',
    consequence:
      'Produced downstream of the activation, not by it: the activation that staged this intent did commit, and everything else it staged went out.',
    source: 'core/dofn.py · REASON_INTENT_DEAD_LETTER',
    detail: 'JSON — {reason, intent_id, seq, tool_name}.',
  },
};

export function reasonInfo(reason: string): ReasonInfo | null {
  return REASON_INFO[reason] ?? null;
}

/* -- Sub-reasons ----------------------------------------------------------- */

/**
 * The two reasons whose `detail` string carries a second, narrower code.
 *
 * `orphaned_result` names which of the four admission checks refused the
 * result; `intent_dead_letter` carries a JSON object whose `reason` names the
 * serialization failure. Both are worth reading as their own axis: "a result
 * arrived late" and "the intent id was never staged" are different bugs with
 * the same reason on the record.
 */
export const ORPHANED_SUB_REASONS: { code: string; meaning: string }[] = [
  {
    code: 'no_continuation',
    meaning:
      'The key held no suspended activation — it already resumed, completed, or never suspended.',
  },
  {
    code: 'unknown_intent',
    meaning: 'A continuation was live, but it never staged the intent id this result answers.',
  },
  {
    code: 'deadline_passed',
    meaning: "The continuation's own deadline had already elapsed when the result arrived.",
  },
  {
    code: 'intent_expired',
    meaning:
      'The intent had passed its own expiry, so admitting the result would break the expiry guard.',
  },
];

const ORPHANED_CODES = new Set(ORPHANED_SUB_REASONS.map((entry) => entry.code));

/** True for the reasons whose detail string is structured rather than prose. */
export function hasSubReasons(reason: string): boolean {
  return reason === 'orphaned_result' || reason === 'intent_dead_letter';
}

/**
 * The sub-reason carried by one record's `detail`, or null when it carries none.
 *
 * Null is a real answer here, not a parse failure to hide: a record whose detail
 * does not name one of the four codes is a record the console cannot classify
 * further, and saying so is more useful than bucketing it under a guess.
 */
export function subReasonOf(reason: string, detail: string): string | null {
  if (!detail) return null;
  if (reason === 'orphaned_result') {
    const head = detail.split(':', 1)[0]?.trim() ?? '';
    return ORPHANED_CODES.has(head) ? head : null;
  }
  if (reason === 'intent_dead_letter') {
    try {
      const parsed: unknown = JSON.parse(detail);
      if (parsed && typeof parsed === 'object' && 'reason' in parsed) {
        const value = (parsed as { reason: unknown }).reason;
        return typeof value === 'string' && value ? value : null;
      }
    } catch {
      // Not the documented JSON. Unclassified, which the caller renders as such.
    }
    return null;
  }
  return null;
}

/** The intent id an `orphaned_result` detail points at, when it carries one. */
export function orphanedIntentId(detail: string): string | null {
  const index = detail.indexOf(':');
  if (index < 0) return null;
  const tail = detail.slice(index + 1).trim();
  return tail || null;
}
