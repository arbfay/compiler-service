type LogLevel = 'info' | 'warn' | 'error' | 'debug';

/** Structured console logger with consistent formatting. */
export class ConsoleLogger {
  constructor(
    private prefix = 'compiler-service',
    private baseMeta?: Record<string, unknown>
  ) {}

  private fmt(level: LogLevel, message: string, meta?: Record<string, unknown>) {
    const ts = new Date().toISOString();
    const payload = { ts, level, service: this.prefix, ...this.baseMeta, message, ...meta };
    return JSON.stringify(payload);
  }

  info(message: string, meta?: Record<string, unknown>) {
    console.info(this.fmt('info', message, meta));
  }

  warn(message: string, meta?: Record<string, unknown>) {
    console.warn(this.fmt('warn', message, meta));
  }

  error(message: string, meta?: Record<string, unknown>) {
    console.error(this.fmt('error', message, meta));
  }

  debug(message: string, meta?: Record<string, unknown>) {
    console.debug(this.fmt('debug', message, meta));
  }

  /** Create a logger that automatically attaches the provided trace id. */
  withTrace(traceId: string) {
    return new ConsoleLogger(this.prefix, { ...this.baseMeta, traceId });
  }
}

export const logger = new ConsoleLogger();
