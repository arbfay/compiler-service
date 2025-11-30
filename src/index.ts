import { Hono, type Context } from 'hono';
import { ComputeGraph, buildQuery } from './compiler';
import { DEFAULT_CONFIG } from './settings';
import { randomUUID } from 'crypto';
import { logger, type ConsoleLogger } from './middleware/logging';
import { AppError, BadRequestError, ValidationError } from './errors';
import { userQuerySchema } from './schemas';
import type { ContentfulStatusCode } from 'hono/utils/http-status';

const app = new Hono<{ Variables: { traceId: string; logger: ConsoleLogger } }>();
const HTTP_STATUS_OK: ContentfulStatusCode = 200;
const HTTP_STATUS_NOT_FOUND: ContentfulStatusCode = 404;
const HTTP_STATUS_INTERNAL_ERROR: ContentfulStatusCode = 500;

// Trace ID + request-scoped logger middleware.
app.use('*', async (c, next) => {
  const traceId = c.req.header('cf-ray') || randomUUID();
  const reqLogger = logger.withTrace(traceId);

  c.set('traceId', traceId);
  c.set('logger', reqLogger);
  c.header('x-trace-id', traceId);

  const started = performance.now();
  reqLogger.info('request.start', { method: c.req.method, path: c.req.path });
  try {
    await next();
  } catch (err) {
    reqLogger.error('request.error', { error: err instanceof Error ? err.message : String(err) });
    throw err;
  } finally {
    const elapsed = Math.round(performance.now() - started);
    const status = c.res?.status ?? HTTP_STATUS_OK;
    reqLogger.info('request.end', { status, elapsed });
  }
});

app.get('/health', (c) => {
  return c.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.post('/compile', async (c) => {
  const reqLogger = c.get('logger');
  try {
    // Parse request body
    let body;
    try {
      body = await c.req.json();
    } catch (parseError) {
      reqLogger.warn('compile.parse_error', {
        error: parseError instanceof Error ? parseError.message : 'unknown',
      });
      throw new BadRequestError('Invalid JSON in request body', {
        cause: parseError instanceof Error ? parseError.message : 'unknown',
      });
    }

    const parsed = userQuerySchema.safeParse(body);
    if (!parsed.success) {
      const errs = parsed.error.issues.map((i) => `${i.path.join('.') || 'root'}: ${i.message}`);
      reqLogger.warn('compile.validation_failed', { errors: errs });
      throw new ValidationError(errs);
    }
    const screener = parsed.data;

    // Build the compute graph
    const graph = new ComputeGraph(screener, DEFAULT_CONFIG);

    // Optimize the graph
    graph.optimize();

    // Generate mermaid diagram
    const mermaid = graph.toMermaid();

    // Generate SQL query
    const result = buildQuery(screener);
    reqLogger.info('compile.success', {
      screenerId: screener.id,
      graphNodes: Object.keys(graph.getNodes()).length,
      parameterCount: Object.keys(result.parameters).length,
    });

    // Return response
    return c.json(
      {
        success: true,
        query: {
          id: screener.id,
          name: screener.name,
        },
        graph: mermaid,
        sql: {
          query: result.query,
          parameters: result.parameters,
        },
      },
      HTTP_STATUS_OK,
      {
        'Cache-Control': 'no-store',
      }
    );
  } catch (error) {
    return respondError(c, error, reqLogger);
  }
});

app.notFound((c) =>
  c.json(
    {
      error: 'not_found',
      message: 'Not Found',
      path: c.req.path,
      requestId: c.get('traceId'),
    },
    HTTP_STATUS_NOT_FOUND
  )
);

app.onError((err, c) => {
  const reqLogger =
    (c.get('logger') as ConsoleLogger | undefined) ?? logger.withTrace(c.get('traceId'));
  return respondError(c, err, reqLogger);
});

function respondError(
  c: Context<{ Variables: { traceId: string; logger: ConsoleLogger } }>,
  err: unknown,
  reqLogger: ConsoleLogger
) {
  if (err instanceof AppError) {
    const level = err.status >= 500 ? 'error' : 'warn';
    reqLogger[level](err.code, { message: err.message, details: err.details });
    return c.json(
      {
        error: err.code,
        message: err.message,
        details: err.details,
        requestId: c.get('traceId'),
      },
      err.status as ContentfulStatusCode
    );
  }

  reqLogger.error('internal_error', { error: err instanceof Error ? err.message : String(err) });
  return c.json(
    {
      error: 'internal_error',
      message: 'Internal server error',
      requestId: c.get('traceId'),
    },
    HTTP_STATUS_INTERNAL_ERROR
  );
}

export default app;
