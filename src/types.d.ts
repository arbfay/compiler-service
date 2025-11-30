declare global {
  type TimeUnit = 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year';
  type TimeUnitType = 'calendar' | 'trading';

  interface BaseTimeRange {
    type: string;
  }

  // Prefer absolute time ranges for queries that include something like "over the last X months"
  interface AbsoluteTimeRange extends BaseTimeRange {
    type: 'absolute';
    from: number; // Unix timestamp
    to: number; // Unix timestamp
  }

  interface RelativeTimeRange extends BaseTimeRange {
    type: 'relative';
    duration: number;
    unit: TimeUnit;
    at?: number; // Optional end time, defaults to current time
  }

  interface TradingTimeRange extends BaseTimeRange {
    type: 'trading'; // Trading time range => only includes trading days/hours/minutes (window functions)
    duration: number;
    unit: TimeUnit;
    at?: number;
  }

  type TimeRange = AbsoluteTimeRange | RelativeTimeRange | TradingTimeRange;

  interface BaseFilter {
    type: string;
    comment?: string;
  }
  // Define specific filter interfaces
  interface SimpleFilter extends BaseFilter {
    type: 'simple';
    target: Expression;
    op: FilterOperator;
    value: Expression;
  }

  type LogicalOperator = 'and' | 'or' | 'not';

  interface CompositeFilter extends BaseFilter {
    type: 'composite';
    operator: LogicalOperator;
    filters: Filter[];
  }

  type Filter = SimpleFilter | CompositeFilter;

  // Basic operators and expressions
  type FilterOperator =
    | 'eq'
    | 'neq' // Equality
    | 'gt'
    | 'gte' // Greater than
    | 'lt'
    | 'lte' // Less than
    | 'in'
    | 'nin' // List inclusion
    | 'contains'
    | 'ncontains'; // String containment

  type MathOperator =
    | '+'
    | '-'
    | '*'
    | '/' // Basic arithmetic
    | '^'
    | '%' // Power and modulo
    | 'sqrt'
    | 'abs' // Mathematical functions
    | 'ln'
    | 'log10' // Logarithms
    | '>'
    | '>='
    | '<'
    | '<=' // Comparison
    | '=='
    | '!=';

  type AggregationType =
    | 'first'
    | 'last'
    | 'min'
    | 'max'
    | 'median'
    | 'percentile'
    | 'avg'
    | 'sum'
    | 'stddev'
    | 'count'
    | 'variance'
    | 'diff'
    | 'diff_pct'
    | 'ema';

  interface BaseExpression {
    type: 'constant' | 'math' | 'aggregate' | 'metric';
    alias?: string;
    comment?: string; // Optional comment for information purposes
  }

  interface MetricExpression extends BaseExpression {
    type: 'metric';
    metric: string;
    filter?: Filter; // Optional filter for non-UserQuery tasks (like enrichment and databoard)
  }

  interface ConstantExpression extends BaseExpression {
    type: 'constant';
    value: number | string | boolean | string[] | number[];
  }

  interface MathExpression extends BaseExpression {
    type: 'math';
    operator: MathOperator;
    operands: Expression[];
  }

  interface AggregateExpression extends BaseExpression {
    type: 'aggregate';
    target: MetricExpression | MathExpression | AggregateExpression;
    aggregation: AggregationType;
    time_range?: TimeRange; // Required for all screeners, to make sure we don't do it on the entire dataset. But can be left out for databoard for simple aggregations like "count" on "sector"
    params?: Record<string, number>; // Additional aggregation parameters, like quantiles or alpha for ema
    filter?: Filter; // For conditional aggregation
  }
  type Expression = ConstantExpression | MathExpression | AggregateExpression | MetricExpression;

  // Grouping capabilities
  type GroupingCriteria =
    | {
        dimension: string;
        limit: number; // For top-N queries
        expression?: MathExpression | AggregateExpression;
      }
    | string;

  // Sorting capabilities
  type SortDirection = 'asc' | 'desc';
  type SortCriteria = {
    expression: Expression;
    direction: SortDirection;
    comment?: string;
  };

  // Main user query interface
  interface UserQuery {
    id: string;
    name: string;
    description?: string;

    // Single top-level filter (usually composite for complex conditions)
    filter: Filter;
    group_by?: GroupingCriteria[];

    // Target markets and assets (one of them should be present)
    markets?: string[];

    // Execution schedule
    frequency_in_sec?: number;
    last_run_at?: number;
    next_run_at?: number;

    // Query state
    status: 'active' | 'running' | 'completed' | 'failed' | 'stopped';

    // Result ordering
    sort_by?: SortCriteria[];
    limit?: number;
  }

  // Type Definitions
  interface TableConfig {
    name: string;
    timeColumn?: string; // Optional - only for time series tables
    timeWindowsAvailable: string[]; // Available time windows for this table
    validMetrics: Set<string>;
    primaryKeys: string[]; // For join conditions
    otherColumns?: string[];
    alwaysIncludeColumns?: string[];
  }

  interface ColumnConfig {
    table: string;
    column: string;
    type: 'Float64' | 'String' | 'UInt8' | 'Date' | 'DateTime' | 'Array(String)';
    joinStrategy?: 'latest' | 'exact_match'; // How to join this table
    timeseries: boolean; // Whether this column is a timeseries column (e.g., price)
  }

  interface QueryBuilderConfig {
    tables: Record<string, TableConfig>;
    columnMappings: Record<string, ColumnConfig>; // Maps metric names to actual columns
    timeFormat: 'date' | 'timestamp';
    maxTimeseriesWindow: number;
    maxLimit: number;
  }

  interface ColumnInfo {
    name: string;
    type: string;
    isKey: boolean;
    cardinality: 'one' | 'many';
  }

  type NodeType =
    | 'source'
    | 'filter'
    | 'composite-filter'
    | 'projection'
    | 'aggregation'
    | 'expression'
    | 'sort'
    | 'limit'
    | 'join'
    | 'aggregate';
  type NodeID = string;

  interface BaseNode {
    id: NodeID;
    type: NodeType;
    inputs: NodeID[]; // References to parent node IDs
    isTerminal?: boolean; // Whether this node is a terminal node (i.e. not referenced by any other node)
    metadata?: NodeMetadata; // Can be used for optimisation hints
  }

  interface SourceNode extends BaseNode {
    type: 'source';
    table: string;
    alias?: string;
    timeColumn?: string;
    timeFormat?: 'date' | 'timestamp';
    columns?: ColumnInfo[];
  }

  interface ProjectionNode extends BaseNode {
    type: 'projection';
    columns: (
      | {
          name: string;
          alias?: string;
          sourceNode?: NodeID;
        }
      | {
          expression: Expression;
          alias?: string;
          sourceNode?: NodeID;
        }
    )[];
  }

  interface ExpressionNode extends BaseNode {
    type: 'expression';
    expression: Expression;
    alias?: string;
  }

  interface FilterNode extends BaseNode {
    type: 'filter';
    condition: {
      left: { input: NodeID; metric: string } | { parameter: string } | Expression;
      right: { input: NodeID; metric: string } | { parameter: string } | Expression;
      op: string;
    };
  }

  interface CompositeFilterNode extends BaseNode {
    type: 'composite-filter';
    operator: 'and' | 'or' | 'not';
  }

  interface SortNode extends BaseNode {
    type: 'sort';
    criteria: {
      expression: NodeID;
      direction: 'asc' | 'desc';
    }[];
  }

  interface LimitNode extends BaseNode {
    type: 'limit';
    limit: number;
  }

  interface JoinCondition {
    left: { input: NodeID; column: string };
    right: { input: NodeID; column: string };
    op: '=' | '!=' | '>' | '>=' | '<' | '<=';
  }

  interface JoinNode extends BaseNode {
    type: 'join';
    joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
    joinConditions: JoinCondition[];
    /** We allow >2 inputs : a single multi-way join node */
    inputs: NodeID[]; // ← already present in BaseNode
  }

  type ComputeNode =
    | SourceNode
    | ProjectionNode
    | ExpressionNode
    | FilterNode
    | CompositeFilterNode
    | SortNode
    | LimitNode
    | JoinNode;

  interface NodeMetadata {
    isGrouping?: boolean;
    isGrouped?: boolean;
    groupDimension?: string;
    offset?: number;
    limit?: number;
    isRequiredProjection?: boolean;
    hasParameter?: boolean;
    [key: string]: unknown;
  }

  interface TranslationContext {
    config: QueryBuilderConfig;
    nodes: Record<NodeID, ComputeNode>;
    visited: Set<NodeID>; // To track visited nodes and avoid redundant processing
    cteGroups?: NodeGroup[];
    timeRanges?: TimeRange[];
  }

  type NodeGroup = {
    id: NodeID;
    nodes: NodeID[];
  };
}

export {};
