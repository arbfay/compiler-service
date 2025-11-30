import { describe, test, expect } from 'bun:test';
import { ComputeGraph } from '../src/compiler';
import { DEFAULT_CONFIG } from '../src/settings';

describe('Graph Optimization', () => {
  test('eliminates duplicate aggregate expression nodes while keeping filters', () => {
    const query: UserQuery = {
      id: 'test',
      name: 'Test',
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
              aggregation: 'avg',
              time_range: { type: 'relative', duration: 30, unit: 'day' },
            },
            op: 'gt',
            value: { type: 'constant', value: 100 },
          },
          {
            type: 'simple',
            target: {
              type: 'aggregate',
              target: { type: 'metric', metric: 'close' },
              aggregation: 'avg',
              time_range: { type: 'relative', duration: 30, unit: 'day' },
            },
            op: 'lt',
            value: { type: 'constant', value: 200 },
          },
        ],
      },
    };

    const graph = new ComputeGraph(query, DEFAULT_CONFIG);
    graph.optimize();

    const nodes = Object.values(graph.getNodes());
    const aggregateExprs = nodes.filter(
      (n): n is ExpressionNode => n.type === 'expression' && n.expression.type === 'aggregate'
    );
    const filterNodes = nodes.filter((n) => n.type === 'filter' || n.type === 'composite-filter');

    expect(aggregateExprs.length).toBe(1);
    expect(filterNodes.length).toBeGreaterThan(0);
  });

  test('retains required nodes after optimization', () => {
    const query: UserQuery = {
      id: 'test',
      name: 'Test',
      status: 'active',
      filter: {
        type: 'simple',
        target: { type: 'metric', metric: 'sector' },
        op: 'eq',
        value: { type: 'constant', value: 'Technology' },
      },
      sort_by: [
        {
          expression: {
            type: 'aggregate',
            target: { type: 'metric', metric: 'close' },
            aggregation: 'avg',
            time_range: { type: 'relative', duration: 30, unit: 'day' },
          },
          direction: 'desc',
        },
      ],
      limit: 10,
    };

    const graph = new ComputeGraph(query, DEFAULT_CONFIG);
    graph.optimize();

    const nodes = Object.values(graph.getNodes());
    const aggregates = nodes.filter(
      (n): n is ExpressionNode => n.type === 'expression' && n.expression.type === 'aggregate'
    );

    expect(nodes.some((n) => n.type === 'filter')).toBe(true);
    expect(nodes.some((n) => n.type === 'sort')).toBe(true);
    expect(nodes.some((n) => n.type === 'limit')).toBe(true);
    expect(aggregates.length).toBe(1);
  });

  test('merges related filters into a streamlined structure', () => {
    const query: UserQuery = {
      id: 'test',
      name: 'Test',
      status: 'active',
      filter: {
        type: 'composite',
        operator: 'and',
        filters: [
          {
            type: 'simple',
            target: { type: 'metric', metric: 'sector' },
            op: 'eq',
            value: { type: 'constant', value: 'Tech' },
          },
          {
            type: 'simple',
            target: { type: 'metric', metric: 'country' },
            op: 'eq',
            value: { type: 'constant', value: 'US' },
          },
        ],
      },
    };

    const graph = new ComputeGraph(query, DEFAULT_CONFIG);
    const before = Object.values(graph.getNodes()).filter(
      (n) => n.type === 'filter' || n.type === 'composite-filter'
    ).length;

    graph.optimize();

    const afterNodes = Object.values(graph.getNodes());
    const afterFilters = afterNodes.filter(
      (n) => n.type === 'filter' || n.type === 'composite-filter'
    );

    expect(afterFilters.length).toBeLessThanOrEqual(before);
    expect(afterFilters.length).toBeGreaterThan(0);
  });
});
