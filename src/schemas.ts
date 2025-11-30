import { z } from 'zod';

const timeUnitSchema = z.enum(['second', 'minute', 'hour', 'day', 'week', 'month', 'year']);

const timeRangeSchema: z.ZodType<TimeRange> = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('absolute'),
    from: z.number().finite(),
    to: z.number().finite(),
  }),
  z.object({
    type: z.literal('relative'),
    duration: z.number().positive(),
    unit: timeUnitSchema,
    at: z.number().nonnegative().optional(),
  }),
  z.object({
    type: z.literal('trading'),
    duration: z.number().positive(),
    unit: timeUnitSchema,
    at: z.number().nonnegative().optional(),
  }),
]);

const filterOperatorSchema = z.enum([
  'eq',
  'neq',
  'gt',
  'gte',
  'lt',
  'lte',
  'in',
  'nin',
  'contains',
  'ncontains',
]);
const mathOperatorSchema = z.enum([
  '+',
  '-',
  '*',
  '/',
  '^',
  '%',
  'sqrt',
  'abs',
  'ln',
  'log10',
  '>',
  '>=',
  '<',
  '<=',
  '==',
  '!=',
]);
const aggregationTypeSchema = z.enum([
  'first',
  'last',
  'min',
  'max',
  'median',
  'percentile',
  'avg',
  'sum',
  'stddev',
  'count',
  'variance',
  'diff',
  'diff_pct',
  'ema',
]);

const constantExpressionSchema: z.ZodType<ConstantExpression> = z.object({
  type: z.literal('constant'),
  value: z.union([z.string(), z.number(), z.boolean(), z.array(z.string()), z.array(z.number())]),
  alias: z.string().min(1).optional(),
  comment: z.string().optional(),
});

const expressionSchema: z.ZodType<Expression> = z.lazy(() =>
  z.union([
    constantExpressionSchema,
    metricExpressionSchema,
    mathExpressionSchema,
    aggregateExpressionSchema,
  ])
);

const simpleFilterSchema = z.object({
  type: z.literal('simple'),
  target: expressionSchema,
  op: filterOperatorSchema,
  value: expressionSchema,
  comment: z.string().optional(),
});

const filterSchema: z.ZodType<Filter> = z.lazy(() =>
  z.discriminatedUnion('type', [
    simpleFilterSchema,
    z.object({
      type: z.literal('composite'),
      operator: z.enum(['and', 'or', 'not']),
      filters: z.array(filterSchema).min(1),
      comment: z.string().optional(),
    }),
  ])
);

const metricExpressionSchema: z.ZodType<MetricExpression> = z.lazy(() =>
  z.object({
    type: z.literal('metric'),
    metric: z.string().min(1),
    alias: z.string().min(1).optional(),
    comment: z.string().optional(),
    filter: filterSchema.optional(),
  })
);

const mathExpressionSchema: z.ZodType<MathExpression> = z.lazy(() =>
  z.object({
    type: z.literal('math'),
    operator: mathOperatorSchema,
    operands: z.array(expressionSchema).min(1),
    alias: z.string().min(1).optional(),
    comment: z.string().optional(),
  })
);

const aggregateExpressionSchema: z.ZodType<AggregateExpression> = z.lazy(() =>
  z.object({
    type: z.literal('aggregate'),
    target: z.union([metricExpressionSchema, mathExpressionSchema, aggregateExpressionSchema]),
    aggregation: aggregationTypeSchema,
    time_range: timeRangeSchema.optional(),
    params: z.record(z.number()).optional(),
    filter: filterSchema.optional(),
    alias: z.string().min(1).optional(),
    comment: z.string().optional(),
  })
);

const groupBySchema: z.ZodType<GroupingCriteria> = z.union([
  z.string().min(1),
  z.object({
    dimension: z.string().min(1),
    limit: z.number().int().positive(),
    expression: z.union([mathExpressionSchema, aggregateExpressionSchema]).optional(),
  }),
]);

const sortCriteriaSchema: z.ZodType<SortCriteria> = z.object({
  expression: expressionSchema,
  direction: z.enum(['asc', 'desc']),
  comment: z.string().optional(),
});

export const userQuerySchema: z.ZodType<UserQuery> = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  description: z.string().optional(),
  created_at: z.number().int().nonnegative().optional(),
  filter: filterSchema,
  group_by: z.array(groupBySchema).optional(),
  sort_by: z.array(sortCriteriaSchema).optional(),
  limit: z.number().int().positive().optional(),
  status: z.enum(['active', 'running', 'completed', 'failed', 'stopped']),
  markets: z.array(z.string().min(1)).optional(),
  frequency_in_sec: z.number().int().positive().optional(),
  last_run_at: z.number().int().nonnegative().optional(),
  next_run_at: z.number().int().nonnegative().optional(),
});

export { filterSchema, groupBySchema };
