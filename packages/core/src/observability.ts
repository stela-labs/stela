import type {
  RuntimeLogger,
  RuntimeLogEvent,
  RuntimeLogLevel,
  RuntimeMetrics,
} from "./types.js";

const LOG_LEVEL_ORDER: Record<RuntimeLogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  silent: 50,
};

type EmittedLogLevel = Exclude<RuntimeLogLevel, "silent">;

export interface RuntimeObserverOptions {
  /** Minimum log level to emit. Default: info. */
  logLevel?: RuntimeLogLevel;
  /** Emit default logs as structured JSON. */
  jsonLogs?: boolean;
  /** Custom logger sink. */
  logger?: RuntimeLogger;
  /** Optional metrics hooks. */
  metrics?: RuntimeMetrics;
}

export interface RuntimeObserver {
  /** Emit a structured log event when enabled by log level. */
  log: (
    level: EmittedLogLevel,
    event: string,
    message: string,
    fields?: Omit<RuntimeLogEvent, "level" | "event" | "message" | "timestamp">,
  ) => void;
  /** Increment a metric counter. */
  increment: (name: string, tags?: Record<string, string>) => void;
  /** Record a metric duration in milliseconds. */
  timing: (name: string, valueMs: number, tags?: Record<string, string>) => void;
  /** Record a metric gauge value. */
  gauge: (name: string, value: number, tags?: Record<string, string>) => void;
}

function defaultLogger(jsonLogs: boolean | undefined): RuntimeLogger {
  return (event) => {
    if (jsonLogs) {
      const line = JSON.stringify(event);
      if (event.level === "error") {
        console.error(line);
      } else if (event.level === "warn") {
        console.warn(line);
      } else {
        console.log(line);
      }
      return;
    }

    const prefix = `[stela:${event.level}] ${event.event}`;
    const suffix = event.error ? ` ${event.error}` : "";
    if (event.level === "error") {
      console.error(`${prefix} ${event.message}${suffix}`);
    } else if (event.level === "warn") {
      console.warn(`${prefix} ${event.message}${suffix}`);
    } else {
      console.log(`${prefix} ${event.message}${suffix}`);
    }
  };
}

/** Create runtime logging and metrics helpers. */
export function createRuntimeObserver(opts: RuntimeObserverOptions = {}): RuntimeObserver {
  const logLevel = opts.logLevel ?? "info";
  const logger = opts.logger ?? defaultLogger(opts.jsonLogs);

  return {
    log(level, event, message, fields = {}) {
      if (LOG_LEVEL_ORDER[level] < LOG_LEVEL_ORDER[logLevel]) return;
      logger({
        level,
        event,
        message,
        timestamp: new Date().toISOString(),
        ...fields,
      });
    },
    increment(name, tags) {
      opts.metrics?.increment?.(name, tags);
    },
    timing(name, valueMs, tags) {
      opts.metrics?.timing?.(name, valueMs, tags);
    },
    gauge(name, value, tags) {
      opts.metrics?.gauge?.(name, value, tags);
    },
  };
}
