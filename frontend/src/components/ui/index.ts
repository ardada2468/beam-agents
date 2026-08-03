/**
 * The primitive barrel. Pages import from `@/components/ui`, never from the
 * file inside — so a primitive can be split out of `primitives.tsx` later
 * without touching a single page.
 */
export * from './primitives';
export { default as TimeSeries } from './TimeSeries';
export type { SeriesSpec } from './TimeSeries';
