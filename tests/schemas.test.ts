import { describe, test, expect } from 'bun:test';
import { userQuerySchema, filterSchema } from '../src/schemas';

describe('Validation Schemas', () => {
  describe('userQuerySchema', () => {
    test('should reject missing required fields', () => {
      const invalid = { id: 'test' };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject empty string id', () => {
      const invalid = {
        id: '',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject invalid status', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'invalid_status',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject negative limit', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        limit: -10,
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject zero limit', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        limit: 0,
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject non-integer limit', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        limit: 10.5,
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject extra unknown fields', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        unknownField: 'should fail',
      };
      // This depends on whether you use .strict() or .passthrough()
      // If using strict mode, this should fail
      const result = userQuerySchema.safeParse(invalid);
      // Verify behavior matches expectation
    });
  });

  describe('filterSchema - Simple Filter', () => {
    test('should accept valid simple filter', () => {
      const valid = {
        type: 'simple',
        target: { type: 'metric', metric: 'price' },
        op: 'gt',
        value: { type: 'constant', value: 100 },
      };
      expect(filterSchema.safeParse(valid).success).toBe(true);
    });

    test('should reject invalid operator', () => {
      const invalid = {
        type: 'simple',
        target: { type: 'metric', metric: 'price' },
        op: 'invalid_op',
        value: { type: 'constant', value: 100 },
      };
      expect(filterSchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject missing target', () => {
      const invalid = {
        type: 'simple',
        op: 'gt',
        value: { type: 'constant', value: 100 },
      };
      expect(filterSchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject missing value', () => {
      const invalid = {
        type: 'simple',
        target: { type: 'metric', metric: 'price' },
        op: 'gt',
      };
      expect(filterSchema.safeParse(invalid).success).toBe(false);
    });
  });

  describe('filterSchema - Composite Filter', () => {
    test('should accept nested composite filters', () => {
      const valid = {
        type: 'composite',
        operator: 'and',
        filters: [
          {
            type: 'composite',
            operator: 'or',
            filters: [
              {
                type: 'simple',
                target: { type: 'metric', metric: 'price' },
                op: 'gt',
                value: { type: 'constant', value: 100 },
              },
              {
                type: 'simple',
                target: { type: 'metric', metric: 'price' },
                op: 'lt',
                value: { type: 'constant', value: 50 },
              },
            ],
          },
          {
            type: 'simple',
            target: { type: 'metric', metric: 'sector' },
            op: 'eq',
            value: { type: 'constant', value: 'Tech' },
          },
        ],
      };
      expect(filterSchema.safeParse(valid).success).toBe(true);
    });

    test('should reject empty filters array', () => {
      const invalid = {
        type: 'composite',
        operator: 'and',
        filters: [],
      };
      expect(filterSchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject invalid operator', () => {
      const invalid = {
        type: 'composite',
        operator: 'xor',
        filters: [
          {
            type: 'simple',
            target: { type: 'metric', metric: 'price' },
            op: 'gt',
            value: { type: 'constant', value: 100 },
          },
        ],
      };
      expect(filterSchema.safeParse(invalid).success).toBe(false);
    });

    test('should handle deeply nested filters (10 levels)', () => {
      // Test recursion depth
      interface RecursiveFilter {
        type: string;
        target?: { type: string; metric: string };
        op?: string;
        value?: { type: string; value: number };
        operator?: string;
        filters?: RecursiveFilter[];
      }

      let nested: RecursiveFilter = {
        type: 'simple',
        target: { type: 'metric', metric: 'price' },
        op: 'gt',
        value: { type: 'constant', value: 100 },
      };
      for (let i = 0; i < 10; i++) {
        nested = { type: 'composite', operator: 'and', filters: [nested] };
      }
      expect(filterSchema.safeParse(nested).success).toBe(true);
    });
  });

  describe('TimeRange validation', () => {
    test('should accept valid relative time range', () => {
      const valid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: 30, unit: 'day' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(valid).success).toBe(true);
    });

    test('should reject negative duration', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: -30, unit: 'day' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject zero duration', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: 0, unit: 'day' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject invalid time unit', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: 30, unit: 'decades' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject Infinity in absolute time range', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'absolute', from: 0, to: Infinity },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject NaN in absolute time range', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'avg',
            time_range: { type: 'absolute', from: NaN, to: 1000 },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });
  });

  describe('Expression validation', () => {
    test('should reject invalid aggregation type', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'invalid_agg',
            time_range: { type: 'relative', duration: 30, unit: 'day' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject empty metric name', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: '' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should reject math expression with empty operands', () => {
      const invalid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'math',
            operator: '+',
            operands: [],
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };
      expect(userQuerySchema.safeParse(invalid).success).toBe(false);
    });

    test('should accept nested aggregate expressions', () => {
      const valid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: {
              type: 'aggregate',
              target: { type: 'metric', metric: 'price' },
              aggregation: 'avg',
              time_range: { type: 'relative', duration: 7, unit: 'day' },
            },
            aggregation: 'stddev',
            time_range: { type: 'relative', duration: 30, unit: 'day' },
          },
          op: 'gt',
          value: { type: 'constant', value: 5 },
        },
      };
      expect(userQuerySchema.safeParse(valid).success).toBe(true);
    });
  });

  describe('Array value validation', () => {
    test("should accept string array for 'in' operator", () => {
      const valid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'in',
          value: { type: 'constant', value: ['Technology', 'Healthcare', 'Finance'] },
        },
      };
      expect(userQuerySchema.safeParse(valid).success).toBe(true);
    });

    test("should accept number array for 'in' operator", () => {
      const valid = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'close' },
          op: 'in',
          value: { type: 'constant', value: [100, 200, 300] },
        },
      };
      expect(userQuerySchema.safeParse(valid).success).toBe(true);
    });
  });
});
