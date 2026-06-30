const UNIT_MS: Record<string, number> = {
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

/**
 * Parse a human-readable duration string into milliseconds.
 *
 * Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
 *
 * @param duration - A duration string such as `"10s"`, `"30m"`, `"1h"`, or `"2d"`.
 * @returns The duration in milliseconds.
 * @throws If the format is invalid or the value is zero.
 *
 * @example
 * parseDuration("30m") // 1_800_000
 */
export function parseDuration(duration: string): number {
  const match = /^(\d+)(s|m|h|d)$/.exec(duration.trim());
  if (!match) {
    throw new Error(
      `Invalid duration "${duration}". Expected format: <number><unit> where unit is s, m, h, or d.`,
    );
  }
  const value = parseInt(match[1], 10);
  if (value === 0) {
    throw new Error(`Invalid duration "${duration}". Duration must be greater than zero.`);
  }
  const unit = match[2];
  return value * UNIT_MS[unit];
}
