import { ComputeGraph } from './compute-graph';
import { QueryBuilder } from './sql/clickhouse';

export { ComputeGraph } from './compute-graph';
export { QueryBuilder } from './sql/clickhouse';
export { renderMermaid } from './mermaid';

const ALIAS_MAX_LENGTH = 65;

/**
 * Helper to evaluate if 2 expressions are the same
 */
export function areExpressionsEqual(expr1: Expression, expr2: Expression): boolean {
  if (expr1.type === expr2.type && expr1.alias === expr2.alias) {
    if (expr1.type === 'constant') {
      return expr1.value === (expr2 as ConstantExpression).value;
    }
    if (expr1.type === 'metric') {
      return expr1.metric === (expr2 as MetricExpression).metric;
    }
    if (expr1.type === 'aggregate') {
      const expr = expr2 as AggregateExpression;
      if (expr1.aggregation === expr.aggregation) {
        if (expr1.time_range && expr.time_range) {
          if (expr1.time_range.type === expr.time_range.type) {
            if (areExpressionsEqual(expr1.target, expr.target)) {
              if (expr1.filter && expr.filter) {
                return areFiltersEqual(expr1.filter, expr.filter);
              } else if (!expr1.filter && !expr.filter) {
                return true;
              }
            }
          }
        } else if (!expr1.time_range && !expr.time_range) {
          if (areExpressionsEqual(expr1.target, expr.target)) {
            if (expr1.filter && expr.filter) {
              return areFiltersEqual(expr1.filter, expr.filter);
            } else if (!expr1.filter && !expr.filter) {
              return true;
            }
          }
        } else if (
          (expr1.time_range && !expr.time_range) ||
          (!expr1.time_range && expr.time_range)
        ) {
          return false;
        }
      }
    }
    if (expr1.type === 'math') {
      const exp = expr2 as MathExpression;
      return (
        expr1.operator === exp.operator &&
        expr1.operands.length === exp.operands.length &&
        expr1.operands.every((op, i) => areExpressionsEqual(op, exp.operands[i]))
      );
    }
  }
  return false;
}

export function areFiltersEqual(filter1: Filter, filter2: Filter): boolean {
  if (filter1.type === filter2.type) {
    if (filter1.type === 'simple') {
      const f1 = filter1 as SimpleFilter;
      const f2 = filter2 as SimpleFilter;
      return (
        areExpressionsEqual(f1.target, f2.target) &&
        areExpressionsEqual(f1.value, f2.value) &&
        f1.op === f2.op
      );
    }
    if (filter1.type === 'composite') {
      const f1 = filter1 as CompositeFilter;
      const f2 = filter2 as CompositeFilter;
      return (
        f1.operator === f2.operator &&
        f1.filters.length === f2.filters.length &&
        f1.filters.every((f, i) => areFiltersEqual(f, f2.filters[i]))
      );
    }
  }
  return false;
}

export function generateAlias(expr?: Expression): string {
  if (!expr) return `col_${Math.random().toString(36).slice(2, 6)}`;

  if ('alias' in expr && expr.alias) return expr.alias;
  if (expr.type === 'metric') return expr.metric;
  if (expr.type === 'constant') return 'constant';

  if (expr.type === 'aggregate') {
    const base = `${expr.aggregation}_${generateAlias(expr.target)}`;
    return expr.time_range ? `${base}_${timeRangeToAlias(expr.time_range)}` : base;
  }
  if (expr.type === 'math') {
    return `${expr.operator}_${expr.operands.map(generateAlias).join('_')}`.slice(
      0,
      ALIAS_MAX_LENGTH
    );
  }
  return `expr_${Math.random().toString(36).slice(2, 6)}`;
}

export function timeRangeToAlias(range: TimeRange): string {
  if (range.type === 'relative' || range.type === 'trading') {
    return `${range.duration}${range.unit}`;
  } else if (range.type === 'absolute') {
    const from = new Date(range.from * 1000).toISOString().split('T')[0].replace(/-/g, '');
    const to = new Date(range.to * 1000).toISOString().split('T')[0].replace(/-/g, '');
    return `${from}_${to}`;
  }
  return '';
}

type ParameterValue = string | number | boolean | string[] | number[];

/**
 * Build a SQL query and parameter map from a validated user query by
 * constructing/optimising the compute graph and translating it.
 */
export function buildQuery(req: UserQuery): {
  query: string;
  parameters: Record<string, ParameterValue>;
} {
  const computeGraph = new ComputeGraph(req);
  computeGraph.optimize();

  const queryBuilder = QueryBuilder.fromComputeGraph(computeGraph);
  return { query: queryBuilder.translateToSQL(), parameters: computeGraph.parameters };
}
