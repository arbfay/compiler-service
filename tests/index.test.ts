import { describe, test, expect } from 'bun:test';
import app from '../src/index';

interface HealthResponse {
  status: string;
  timestamp: string;
}

interface CompileSuccessResponse {
  success: boolean;
  query: {
    id: string;
    name: string;
  };
  graph: string;
  sql: {
    query: string;
    parameters: Record<string, string | number | boolean | string[] | number[]>;
  };
}

interface ErrorResponse {
  error: string;
  message?: string;
  details?: unknown;
  requestId?: string;
}

describe('API Endpoints', () => {
  describe('GET /health', () => {
    test('should return health status', async () => {
      const req = new Request('http://localhost/health');
      const res = await app.fetch(req);

      expect(res.status).toBe(200);

      const data = (await res.json()) as HealthResponse;
      expect(data.status).toBe('ok');
      expect(data.timestamp).toBeDefined();
    });
  });

  describe('POST /compile', () => {
    test('should compile a simple query', async () => {
      const screener = {
        id: 'test-screener',
        created_at: 0,
        name: 'Test Screener',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Technology' },
        },
        limit: 10,
      };

      const req = new Request('http://localhost/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(screener),
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(200);

      const data = (await res.json()) as CompileSuccessResponse;
      expect(data.success).toBe(true);
      expect(data.query.id).toBe('test-screener');
      expect(data.query.name).toBe('Test Screener');
      expect(data.graph).toBeDefined();
      expect(data.graph).toContain('graph TD');
      expect(data.sql.query).toBeDefined();
      expect(data.sql.query).toContain('SELECT');
      expect(data.sql.parameters).toBeDefined();
    });

    test('should compile complex query with aggregations', async () => {
      const screener = {
        id: 'momentum-screener',
        created_at: 0,
        name: 'Momentum Screener',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'close' },
            aggregation: 'diff_pct',
            time_range: { type: 'relative', duration: 30, unit: 'day' },
            alias: 'return_30d',
          },
          op: 'gt',
          value: { type: 'constant', value: 10 },
        },
        sort_by: [
          {
            expression: {
              type: 'aggregate',
              target: { type: 'metric', metric: 'close' },
              aggregation: 'diff_pct',
              time_range: { type: 'relative', duration: 30, unit: 'day' },
              alias: 'return_30d',
            },
            direction: 'desc',
          },
        ],
        limit: 50,
      };

      const req = new Request('http://localhost/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(screener),
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(200);

      const data = (await res.json()) as CompileSuccessResponse;
      expect(data.success).toBe(true);
      expect(data.sql.query).toContain('WITH');
      expect(data.sql.query.toLowerCase()).toMatch(/last_value|first_value/);
    });

    test('should return 422 for missing required fields', async () => {
      const invalidScreener = {
        id: 'test',
        // Missing name and filter
      };

      const req = new Request('http://localhost/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(invalidScreener),
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(422);

      const data = (await res.json()) as ErrorResponse;
      expect(data.error).toBeDefined();
      expect(data.details).toBeDefined();
    });

    test('should return 400 for invalid JSON', async () => {
      const req = new Request('http://localhost/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'invalid json',
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(400);
    });

    test('should handle top-N per group query', async () => {
      const screener = {
        id: 'top-n-per-sector',
        created_at: 0,
        name: 'Top 3 per Sector',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'country' },
          op: 'eq',
          value: { type: 'constant', value: 'United States' },
        },
        group_by: [
          {
            dimension: 'sector',
            expression: {
              type: 'aggregate',
              target: { type: 'metric', metric: 'price' },
              aggregation: 'diff_pct',
              time_range: { type: 'relative', duration: 90, unit: 'day' },
              alias: 'price_change_90d',
            },
            limit: 3,
          },
        ],
        limit: 100,
      };

      const req = new Request('http://localhost/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(screener),
      });

      const res = await app.fetch(req);
      expect(res.status).toBe(200);

      const data = (await res.json()) as CompileSuccessResponse;
      expect(data.success).toBe(true);
      expect(data.sql.query).toContain('LIMIT');
      expect(data.sql.query).toContain('BY sector');
    });
  });
});
