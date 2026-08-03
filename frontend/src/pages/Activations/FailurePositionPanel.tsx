/**
 * Where the activation was when it failed.
 *
 * The four scalars are nullable and the null is the point: the runtime records
 * them only on the routes that can reach an activation context, and leaves them
 * absent on the timeout route, on failures before the context exists, and on the
 * non-activation routes. So a missing value renders as "not available" — never
 * as `0`, which would claim the activation failed at step zero having staged
 * nothing, a specific and wrong statement.
 */

import { EM_DASH, formatCount } from '@/lib/format';

import type { FailurePosition } from './trace-attrs';
import { positionIsEmpty } from './trace-attrs';

function Cell({
  label,
  value,
  text = false,
}: {
  label: string;
  value: string | null;
  /** A recorded name rather than a figure: monospace, and not headline-sized. */
  text?: boolean;
}) {
  const missing = value === null;
  const classes = [
    'fail__value',
    text && !missing ? 'fail__value--text' : '',
    missing ? 'fail__value--missing' : '',
  ]
    .filter(Boolean)
    .join(' ');
  return (
    <div className="fail__cell">
      <p className="fail__label eyebrow">{label}</p>
      <p className={classes}>{value ?? EM_DASH}</p>
      {missing ? <p className="fail__note">not available on this route</p> : null}
    </div>
  );
}

export function FailurePositionPanel({ position }: { position: FailurePosition }) {
  const empty = positionIsEmpty(position);

  return (
    <div className="fail">
      <div className="fail__grid">
        <Cell
          label="Failure step"
          value={position.step === null ? null : formatCount(position.step)}
        />
        <Cell label="Last event" value={position.lastEvent} text />
        <Cell
          label="Staged intents"
          value={position.stagedIntents === null ? null : formatCount(position.stagedIntents)}
        />
        <Cell
          label="LLM calls"
          value={position.llmCalls === null ? null : formatCount(position.llmCalls)}
        />
      </div>
      <p className="fail__source muted">
        {empty
          ? 'This error came from a route that cannot reach an activation context, so no failure position was recorded. The fields above are absent, not zero.'
          : `Read from ${position.source}.`}
      </p>
    </div>
  );
}
