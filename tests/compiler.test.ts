import { describe, test, expect } from 'bun:test';
import { ComputeGraph, QueryBuilder, buildQuery } from '../src/compiler';
import { DEFAULT_CONFIG } from '../src/settings';

describe('UserQuery Compiler', () => {
  describe('Top N per Sector', () => {
    const topNPerSector: UserQuery = {
      id: 'top-n-per-sector',
      name: 'Top 3 Stocks per Sector by 90-day Price Change',
      status: 'active',
      filter: {
        // Pre-filter for active stocks in US
        type: 'composite',
        operator: 'and',
        filters: [
          {
            type: 'simple',
            target: { type: 'metric', metric: 'active' },
            op: 'eq',
            value: { type: 'constant', value: 1 },
          },
          {
            type: 'simple',
            target: { type: 'metric', metric: 'country' },
            op: 'eq',
            value: { type: 'constant', value: 'United States' },
          },
        ],
      },
      group_by: [
        {
          dimension: 'sector', // Group by sector
          expression: {
            // Order within each sector by this expression
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'diff_pct',
            time_range: { type: 'relative', duration: 90, unit: 'day' },
            alias: 'price_change_90d', // Give it an alias
          },
          limit: 3, // Take top 3
        },
      ],
      sort_by: [
        // Overall sort for the final result (optional, but good for testing)
        { expression: { type: 'metric', metric: 'sector' }, direction: 'asc' },
        {
          expression: {
            // This should match the group_by expression for correct Top-N
            type: 'aggregate',
            target: { type: 'metric', metric: 'price' },
            aggregation: 'diff_pct',
            time_range: { type: 'relative', duration: 90, unit: 'day' },
            alias: 'price_change_90d',
          },
          direction: 'desc',
        },
      ],
      limit: 100, // Overall limit
    };

    test('should create compute graph', () => {
      const graph = new ComputeGraph(topNPerSector, DEFAULT_CONFIG);
      const nodes = graph.getNodes();

      expect(Object.keys(nodes).length).toBeGreaterThan(0);

      // Should have source nodes
      const sourceNodes = Object.values(nodes).filter((n) => n.type === 'source');
      expect(sourceNodes.length).toBeGreaterThan(0);

      // Should have filter nodes
      const filterNodes = Object.values(nodes).filter((n) => n.type === 'filter');
      expect(filterNodes.length).toBeGreaterThan(0);

      // Should have expression nodes
      const expressionNodes = Object.values(nodes).filter((n) => n.type === 'expression');
      expect(expressionNodes.length).toBeGreaterThan(0);
    });

    test('should generate valid SQL', () => {
      const result = buildQuery(topNPerSector);

      expect(result.query).toBeTruthy();
      expect(typeof result.query).toBe('string');

      // Should contain key SQL keywords
      expect(result.query).toContain('SELECT');
      expect(result.query).toContain('FROM');

      // Should reference the tables
      expect(result.query.toLowerCase()).toMatch(/daily_agg|tickers/);

      // Should have the filters (as parameter placeholders)
      expect(result.query).toContain('{param_');

      // Should have LIMIT
      expect(result.query).toContain('LIMIT');
    });

    test('should have correct parameters', () => {
      const result = buildQuery(topNPerSector);

      expect(result.parameters).toBeTruthy();
      expect(typeof result.parameters).toBe('object');

      // Should have parameters for the filter values
      const paramValues = Object.values(result.parameters);
      expect(paramValues).toContain('United States');
      // Note: numeric value 1 may be inlined rather than parameterized
    });

    test('should optimize graph correctly', () => {
      const graph = new ComputeGraph(topNPerSector, DEFAULT_CONFIG);
      const nodesBefore = Object.keys(graph.getNodes()).length;

      graph.optimize();

      const nodesAfter = Object.keys(graph.getNodes()).length;

      // Optimization should reduce or maintain node count
      expect(nodesAfter).toBeLessThanOrEqual(nodesBefore);
    });

    test('should generate mermaid diagram', () => {
      const graph = new ComputeGraph(topNPerSector, DEFAULT_CONFIG);
      const mermaid = graph.toMermaid();

      expect(mermaid).toBeTruthy();
      expect(mermaid).toContain('graph TD');
      expect(mermaid).toMatch(/\(\(AND\)\)/); // logical operator node is rendered explicitly
      expect(mermaid).toContain('tickers'); // source nodes use table names
      expect(mermaid).toContain('daily_agg'); // source nodes use table names
      expect(mermaid).toMatch(/GROUP BY/); // grouping is surfaced explicitly
    });
  });

  describe('Simple Filter Query', () => {
    const simpleScreener: UserQuery = {
      id: 'high-volume',
      name: 'High Volume Stocks',
      status: 'active',
      filter: {
        type: 'simple',
        target: {
          type: 'aggregate',
          target: { type: 'metric', metric: 'volume' },
          aggregation: 'avg',
          time_range: { type: 'relative', duration: 30, unit: 'day' },
        },
        op: 'gt',
        value: { type: 'constant', value: 1000000 },
      },
      limit: 50,
    };

    test('should generate SQL for simple filter', () => {
      const result = buildQuery(simpleScreener);

      expect(result.query).toBeTruthy();
      expect(result.query).toContain('SELECT');
      expect(result.query).toContain('FROM');

      // Should have volume reference
      expect(result.query.toLowerCase()).toContain('volume');
    });
  });

  describe('Price Momentum Query', () => {
    const priceMomentum: UserQuery = {
      id: 'price-momentum',
      name: 'Strong Price Momentum',
      status: 'active',
      filter: {
        type: 'composite',
        operator: 'and',
        filters: [
          {
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
          {
            type: 'simple',
            target: {
              type: 'aggregate',
              target: { type: 'metric', metric: 'close' },
              aggregation: 'diff_pct',
              time_range: { type: 'relative', duration: 90, unit: 'day' },
              alias: 'return_90d',
            },
            op: 'gt',
            value: { type: 'constant', value: 20 },
          },
        ],
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
      limit: 25,
    };

    test('should generate SQL for momentum screener', () => {
      const result = buildQuery(priceMomentum);

      expect(result.query).toBeTruthy();
      expect(result.query).toContain('SELECT');

      // Should have window functions for returns
      expect(result.query.toLowerCase()).toMatch(/last_value|first_value/);
    });
  });

  describe('Sector Filter Query', () => {
    const sectorFilter: UserQuery = {
      id: 'tech-stocks',
      name: 'Technology Stocks',
      status: 'active',
      filter: {
        type: 'simple',
        target: { type: 'metric', metric: 'sector' },
        op: 'eq',
        value: { type: 'constant', value: 'Technology' },
      },
      sort_by: [
        {
          expression: { type: 'metric', metric: 'ticker' },
          direction: 'asc',
        },
      ],
      limit: 100,
    };

    test('should generate SQL for sector filter', () => {
      const result = buildQuery(sectorFilter);

      expect(result.query).toBeTruthy();
      expect(result.query).toContain('SELECT');

      // Should reference sector
      expect(result.query.toLowerCase()).toContain('sector');
      expect(result.parameters).toBeTruthy();
    });
  });

  describe('Math Expression Query', () => {
    const mathExpression: UserQuery = {
      id: 'price-volume-ratio',
      name: 'High Price-to-Volume Ratio',
      status: 'active',
      filter: {
        type: 'simple',
        target: {
          type: 'math',
          operator: '/',
          operands: [
            { type: 'metric', metric: 'close' },
            { type: 'metric', metric: 'volume' },
          ],
          alias: 'price_vol_ratio',
        },
        op: 'gt',
        value: { type: 'constant', value: 0.001 },
      },
      limit: 50,
    };

    test('should generate SQL for math expression', () => {
      const result = buildQuery(mathExpression);

      expect(result.query).toBeTruthy();
      expect(result.query).toContain('SELECT');

      // Should have division operator
      expect(result.query).toContain('/');
    });
  });

  describe('Absolute Time Range Query', () => {
    const absoluteTime: UserQuery = {
      id: 'ytd-performance',
      name: 'Year-to-Date Performance',
      status: 'active',
      filter: {
        type: 'simple',
        target: {
          type: 'aggregate',
          target: { type: 'metric', metric: 'close' },
          aggregation: 'diff_pct',
          time_range: {
            type: 'absolute',
            from: Math.floor(new Date('2024-01-01').getTime() / 1000),
            to: Math.floor(new Date('2024-12-31').getTime() / 1000),
          },
          alias: 'ytd_return',
        },
        op: 'gt',
        value: { type: 'constant', value: 0 },
      },
      limit: 100,
    };

    test('should generate SQL for absolute time range', () => {
      const result = buildQuery(absoluteTime);

      expect(result.query).toBeTruthy();
      expect(result.query).toContain('SELECT');

      // Should have BETWEEN for absolute time range
      expect(result.query).toContain('BETWEEN');
      expect(result.query).toContain('2024');
    });
  });

  describe('ComputeGraph Edge Cases', () => {
    test('should handle empty screener', () => {
      const emptyScreener: UserQuery = {
        id: 'empty',
        name: 'Empty',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'ticker' },
          op: 'eq',
          value: { type: 'constant', value: 'AAPL' },
        },
      };

      expect(() => {
        buildQuery(emptyScreener);
      }).not.toThrow();
    });

    test('should handle execution order', () => {
      const graph = new ComputeGraph(undefined, DEFAULT_CONFIG);

      const source1 = graph.addNode({
        id: 'src_1',
        type: 'source',
        table: 'daily_agg',
        inputs: [],
      });

      const proj1 = graph.addNode({
        id: 'proj_1',
        type: 'projection',
        inputs: [source1],
        columns: [{ name: 'close' }],
      });

      const filter1 = graph.addNode({
        id: 'filter_1',
        type: 'filter',
        inputs: [proj1],
        condition: {
          left: { input: proj1, metric: 'close' },
          right: { type: 'constant', value: 100 },
          op: 'gt',
        },
      });

      const order = graph.getExecutionOrder();

      expect(order.length).toBe(3);
      expect(order[0].id).toBe('src_1');
      expect(order[1].id).toBe('proj_1');
      expect(order[2].id).toBe('filter_1');
    });

    test('should detect cycles', () => {
      const graph = new ComputeGraph(undefined, DEFAULT_CONFIG);

      const node1 = graph.addNode({
        id: 'node_1',
        type: 'source',
        table: 'daily_agg',
        inputs: [],
      });

      // Manually create a cycle (this shouldn't happen in normal usage)
      graph.addNode({
        id: 'node_2',
        type: 'projection',
        inputs: [node1],
        columns: [{ name: 'close' }],
      });

      // Create cycle by modifying inputs directly (testing error handling)
      const nodes = graph.getNodes();
      nodes['node_1'].inputs = ['node_2'];

      expect(() => {
        graph.getExecutionOrder();
      }).toThrow(/Cycle detected/);
    });
  });

  describe('Boundary Conditions', () => {
    test('should handle limit of 1', () => {
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        limit: 1,
      };

      const result = buildQuery(query);
      expect(result.query).toContain('LIMIT 1');
    });

    test('should handle very large limit', () => {
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: 'Tech' },
        },
        limit: 1000000,
      };

      const result = buildQuery(query);
      expect(result.query).toContain('LIMIT');
    });

    test('should handle zero duration at boundary', () => {
      // Edge case: what happens with 1-second time range?
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'close' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: 1, unit: 'second' },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };

      expect(() => buildQuery(query)).not.toThrow();
    });

    test('should handle maximum timestamp', () => {
      const maxTimestamp = 2147483647; // Max 32-bit signed int

      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'close' },
            aggregation: 'last',
            time_range: {
              type: 'absolute',
              from: 0,
              to: maxTimestamp,
            },
          },
          op: 'gt',
          value: { type: 'constant', value: 100 },
        },
      };

      expect(() => buildQuery(query)).not.toThrow();
    });

    test('should handle empty string values', () => {
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: '' },
        },
      };

      const result = buildQuery(query);
      expect(result.parameters).toBeDefined();
    });

    test('should handle very long strings', () => {
      const longString = 'x'.repeat(10000);

      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'sector' },
          op: 'eq',
          value: { type: 'constant', value: longString },
        },
      };

      const result = buildQuery(query);
      expect(result.parameters).toBeDefined();
      expect(Object.values(result.parameters)).toContain(longString);
    });

    test('should handle negative numbers', () => {
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'close' },
          op: 'lt',
          value: { type: 'constant', value: -100 },
        },
      };

      const result = buildQuery(query);
      expect(result.parameters).toBeDefined();
    });

    test('should handle very small decimals', () => {
      const query: UserQuery = {
        id: 'test',
        name: 'Test',
        status: 'active',
        filter: {
          type: 'simple',
          target: { type: 'metric', metric: 'close' },
          op: 'gt',
          value: { type: 'constant', value: 0.000001 },
        },
      };

      const result = buildQuery(query);
      expect(result.parameters).toBeDefined();
    });
  });
});
