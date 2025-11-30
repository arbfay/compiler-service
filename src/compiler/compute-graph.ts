import { DEFAULT_CONFIG } from '../settings';
import { areExpressionsEqual, generateAlias } from '.';
import { renderMermaid } from './mermaid';

/**
 * A compute graph represents a query as a directed acyclic graph (DAG) of nodes.
 *
 * The ComputeGraph class builds and optimizes a computational graph from user queries,
 * transforming high-level query specifications into a structured representation that can
 * be executed efficiently. It supports various node types including sources (tables),
 * filters, projections, aggregations, joins, sorts, and limits.
 *
 * **How it works:**
 *
 * 1. **Construction**: The graph is built from a UserQuery by recursively decomposing
 *    expressions and filters into nodes. Each node represents a computation step and
 *    maintains references to its input nodes, forming a dependency graph.
 *
 * 2. **Node Management**: Nodes are tracked by unique IDs, with automatic terminal status
 *    management. Terminal nodes represent final outputs, while intermediate nodes feed
 *    into downstream computations.
 *
 * 3. **Join Inference**: When multiple source tables are detected, the graph automatically
 *    infers join conditions based on common primary keys defined in the configuration.
 *    A multi-way join node is created and downstream nodes are rewired to reference it.
 *
 * 4. **Optimization**: The graph applies several optimization passes:
 *    - Removes duplicate projections and expressions
 *    - Merges adjacent filters with same inputs
 *    - Inlines constant parameters
 *    - Simplifies unnecessary composite filters
 *    - Attempts risky simplifications when safe (e.g., eliminating redundant joins)
 *
 * 5. **Execution Order**: Provides topological ordering of nodes with cycle detection,
 *    ensuring dependencies are computed before dependent nodes.
 *
 * 6. **Parameterization**: Automatically converts literal values into parameterized
 *    queries with proper type inference for security and query plan caching.
 *
 * The graph can be visualized as a Mermaid diagram and serves as an intermediate
 * representation that can be translated into SQL or other query languages.
 *
 * @example
 * ```typescript
 * const graph = ComputeGraph.fromUserQuery({
 *   filter: { type: 'simple', target: { type: 'metric', metric: 'price' }, op: '>', value: 100 },
 *   sort_by: [{ expression: { type: 'metric', metric: 'volume' }, direction: 'desc' }],
 *   limit: 10
 * });
 * graph.optimize();
 * const mermaid = graph.toMermaid();
 * ```
 */
export class ComputeGraph {
  private userQuery: UserQuery | undefined;

  components: {
    filter?: Filter;
    group_by?: GroupingCriteria[];
    sort_by?: SortCriteria[];
    limit?: number;
  };

  private nodes: Record<NodeID, ComputeNode> = {};
  private nodeTypesCounter: Record<NodeType, number> = {
    source: 0,
    filter: 0,
    'composite-filter': 0,
    projection: 0,
    aggregation: 0,
    expression: 0,
    sort: 0,
    limit: 0,
    join: 0,
    aggregate: 0,
  };
  private config: QueryBuilderConfig;
  parameters: Record<string, string | boolean | number | string[] | number[]> = {};

  generateNodeID(type?: NodeType) {
    if (!type) return Math.random().toString(36).substring(2);
    let count = this.nodeTypesCounter[type] + 1;
    const id = `${type.replace('-', '_')}_${count}`;
    this.nodeTypesCounter[type]++;
    return id;
  }

  constructor(userQuery?: UserQuery, config = DEFAULT_CONFIG) {
    this.userQuery = userQuery;
    this.config = config;
    this.components = {
      filter: userQuery?.filter,
      group_by: userQuery?.group_by,
      sort_by: userQuery?.sort_by,
      limit: userQuery?.limit,
    };
    if (this.userQuery) this.processUserQuery();
  }

  static fromUserQuery(userQuery: UserQuery, config = DEFAULT_CONFIG): ComputeGraph {
    return new ComputeGraph(userQuery, config);
  }

  static fromFilter(
    filter: Filter,
    components?: {
      filter?: Filter;
      group_by?: GroupingCriteria[];
      sort_by?: SortCriteria[];
      limit?: number;
    },
    config = DEFAULT_CONFIG
  ): ComputeGraph {
    let graph = new ComputeGraph(undefined, config);
    graph.components = components || {};
    graph.processFilter(filter);
    graph.inferJoins();
    graph.optimize(true);
    return graph;
  }

  static fromExpression(
    expressions: Expression[],
    components?: {
      filter?: Filter;
      group_by?: GroupingCriteria[];
      sort_by?: SortCriteria[];
      limit?: number;
    },
    config = DEFAULT_CONFIG
  ): ComputeGraph {
    let graph = new ComputeGraph(undefined, config);
    graph.components = components || {};
    expressions.map((expr) => graph.processExpression(expr));
    graph.inferJoins();
    graph.optimize(true);
    return graph;
  }

  addNode(node: ComputeNode): NodeID {
    this.nodes[node.id] = node;
    // Adjust terminal status of inputs
    node.inputs.forEach((inputId) => {
      const inputNode = this.nodes[inputId];
      if (inputNode) inputNode.isTerminal = false;
    });
    node.isTerminal = true; // New nodes are terminal by default
    return node.id;
  }

  removeNode(id: NodeID): void {
    // Adjust terminal status of inputs
    const node = this.nodes[id];
    if (!node) return;

    node.inputs.forEach((inputId) => {
      const inputNode = this.nodes[inputId];
      if (inputNode) {
        const isStillReferenced = Object.values(this.nodes).some((n) => n.inputs.includes(inputId));
        inputNode.isTerminal = !isStillReferenced;
      }
    });

    delete this.nodes[id];
  }

  getNodes(): Record<NodeID, ComputeNode> {
    return this.nodes;
  }

  getExecutionOrder(): ComputeNode[] {
    const orderedNodes: ComputeNode[] = [];
    const visited = new Set<NodeID>();
    const processing = new Set<NodeID>();

    const visit = (nodeId: NodeID) => {
      // Check if we're already processing this node (cycle detection)
      if (processing.has(nodeId)) {
        throw new Error(`Cycle detected in graph involving node ${nodeId}`);
      }

      // Skip if already processed
      if (visited.has(nodeId)) {
        return;
      }

      // Validate node exists
      const node = this.nodes[nodeId];
      if (!node) throw new Error(`Node ${nodeId} referenced but not found in graph`);

      // Mark node as being processed
      processing.add(nodeId);

      // Process all dependencies first
      for (const inputId of node.inputs) {
        if (!this.nodes[inputId]) {
          throw new Error(`Node ${nodeId} references non-existent input ${inputId}`);
        }
        visit(inputId);
      }

      // Mark as fully processed
      processing.delete(nodeId);
      visited.add(nodeId);
      orderedNodes.push(node); // Now safe as we validated node exists
    };

    // Start with source nodes
    const sourceNodes = Object.values(this.nodes)
      .filter((node) => node.type === 'source')
      .map((node) => node.id);

    // Process starting from each source
    for (const nodeId of sourceNodes) {
      visit(nodeId);
    }

    // Process any remaining nodes
    Object.keys(this.nodes).forEach((nodeId) => {
      visit(nodeId);
    });

    return orderedNodes;
  }

  createParameter(value: string | boolean | number | string[] | number[], op?: string): string {
    const paramLength = Object.keys(this.parameters).length;
    const paramName = `param_${paramLength + 1}`;

    let type = 'Float64';
    if (op?.includes('LIKE') && typeof value === 'string') {
      // We will have to prepend and append % to the value
      value = `%${value}%`;
    }

    if (typeof value === 'string') type = 'String';
    if (typeof value === 'number') return String(value);
    if (typeof value === 'boolean') return String(value ? 1 : 0);
    if (Array.isArray(value)) {
      if (value.length === 0) return '[]';
      // Check all the elements in the array are of the same type
      if (!value.every((v) => typeof v === typeof value[0]))
        throw new Error('Mixed type arrays as parameters are not supported.');

      if (typeof value[0] === 'string') type = 'Array(String)';
      else if (typeof value[0] === 'number') type = 'Array(Float64)';
      else if (typeof value[0] === 'boolean') type = 'Array(Boolean)';
      else throw new Error('Unsupported array type');
    }
    this.parameters[paramName] = value;
    return `{${paramName}: ${type}}`;
  }

  processUserQuery() {
    // Builds the graph from the user query
    if (!this.userQuery) throw new Error('No user query provided');
    // 1. Recursively process filter
    this.processFilter(this.userQuery.filter);

    // 2. Process group by
    if (this.userQuery.group_by) this.processGroupBy(this.userQuery.group_by);

    // 3. Process sort
    if (this.userQuery.sort_by) this.processSort(this.userQuery.sort_by);

    // 4. Add limit node
    if (this.userQuery.limit) {
      this.addNode({
        id: this.generateNodeID('limit'),
        type: 'limit',
        inputs: [],
        limit: this.userQuery.limit,
      });
    }

    // Infer JOINs
    this.inferJoins();

    // 6. Ensure required columns are included
    this.ensureRequiredColumns();
  }

  processFilter(filter: Filter, otherInputs?: NodeID[]): NodeID {
    // Process a filter and nodes to the graph

    if (filter.type === 'composite') {
      let inputs: NodeID[] = [];
      inputs = filter.filters.map((filter) => this.processFilter(filter));
      // Add other inputs if provided
      if (otherInputs) {
        inputs.push(...otherInputs);
        // Ensure uniqueness
        inputs = [...new Set(inputs)];
      }

      // Add a composite filter node
      return this.addNode({
        id: this.generateNodeID('composite-filter'),
        type: 'composite-filter',
        inputs,
        operator: filter.operator,
      });
    }

    let tmp = filter as SimpleFilter;

    // Process the target and value expressions
    const targetNodeId = this.processExpression(tmp.target);
    let inputs = [targetNodeId];
    // Process value expression (if it's an expression)
    let valueNodeId: NodeID | Expression = tmp.value;
    if (typeof tmp.value === 'object' && 'type' in tmp.value) {
      valueNodeId = this.processExpression(tmp.value);
      inputs.push(valueNodeId);
    }

    // Add other inputs if provided
    if (otherInputs) {
      inputs.push(...otherInputs);
      // Ensure uniqueness
      inputs = [...new Set(inputs)];
    }

    const targetNode = this.nodes[targetNodeId];

    // Create filter node
    let left;
    let right;
    const getMetricName = (node: ComputeNode): string => {
      if (node.type === 'projection') {
        const col = node.columns[0];
        return col ? col.alias || ('name' in col ? col.name : '') : node.id;
      }
      return node.id;
    };

    if (typeof valueNodeId === 'string') {
      left = { input: targetNodeId, metric: getMetricName(targetNode) };
      right = { input: valueNodeId, metric: valueNodeId };
    } else {
      left = { input: targetNodeId, metric: getMetricName(targetNode) };
      right = valueNodeId as Expression;
    }
    return this.addNode({
      id: this.generateNodeID('filter'),
      type: 'filter',
      inputs,
      condition: {
        left,
        right,
        op: tmp.op,
      },
    });
  }

  processSort(sorts: SortCriteria[]): NodeID {
    // Process each sort expression and track their node IDs
    const sortInputs = sorts.map((sort) => this.processExpression(sort.expression));

    // Create the sort node
    return this.addNode({
      id: this.generateNodeID('sort'),
      type: 'sort',
      inputs: [...new Set(sortInputs)], // Remove duplicates
      criteria: sorts.map((sort, index) => ({
        expression: sortInputs[index],
        direction: sort.direction,
      })),
    });
  }

  processGroupBy(groups: UserQuery['group_by']): NodeID[] {
    if (!groups) return [];
    let toReturn: NodeID[] = [];

    for (const groupBy of groups) {
      if (typeof groupBy === 'string') {
        // Simple group by dimension
        const column = this.config.columnMappings[groupBy];
        if (!column) throw new Error(`Grouping dimension ${groupBy} not found in config`);

        // We'll need the source and a projection
        let sourceNodeId = this.findOrCreateSourceNode(column.table);

        toReturn.push(
          this.addNode({
            id: this.generateNodeID('projection'),
            type: 'projection',
            inputs: [sourceNodeId],
            columns: [
              {
                name: column.column,
                alias: groupBy,
                sourceNode: sourceNodeId,
              },
            ],
            metadata: {
              isGrouping: true,
            },
          })
        );
        continue;
      }

      // Complex TopN grouping
      const { dimension, limit, expression } = groupBy;

      // First, get the grouping dimension
      const dimensionNodeId = this.processGroupBy([dimension])[0];

      // If we have an expression to order by, process it
      let expressionNodeId: NodeID | undefined;
      if (expression) expressionNodeId = this.processExpression(expression);

      // Create a sort node for the ranking
      const sortNodeId = this.addNode({
        id: this.generateNodeID('sort'),
        type: 'sort',
        inputs: [dimensionNodeId, ...(expressionNodeId ? [expressionNodeId] : [])],
        criteria: [
          {
            expression: expressionNodeId || dimensionNodeId,
            direction: 'desc', // Default to descending for TopN
          },
        ],
        metadata: {
          isGrouped: true,
          groupDimension: dimension,
          limit,
        },
      });

      // Create a limit node for the TopN
      toReturn.push(
        this.addNode({
          id: this.generateNodeID('limit'),
          type: 'limit',
          inputs: [sortNodeId],
          limit,
          metadata: {
            isGrouped: true,
            groupDimension: dimension,
          },
        })
      );
    }

    return toReturn;
  }

  // Helper method to find or create source nodes
  private findOrCreateSourceNode(table: string): NodeID {
    // Look for existing source node for this table
    const existingNode = Object.entries(this.nodes).find(
      ([_, node]) => node.type === 'source' && (node as SourceNode).table === table
    );

    if (existingNode) {
      return existingNode[0];
    }

    // Create new source node
    return this.addNode({
      id: this.generateNodeID('source'),
      type: 'source',
      table,
      inputs: [],
      timeColumn: this.config.tables[table]?.timeColumn,
    });
  }

  /**
   * Decomposes an expression into compute graph nodes (sources, projections,
   * aggregations) while wiring dependencies; used recursively to build the graph.
   */
  processExpression(expression: Expression, calledFromAgg = false): NodeID {
    switch (expression.type) {
      case 'metric': {
        // Find source for metric
        const column = this.config.columnMappings[expression.metric];
        if (!column) throw new Error(`Metric ${expression.metric} not found in config`);

        // Create source node if doesn't exist
        let sourceNodeId = this.findOrCreateSourceNode(column.table);

        if (!sourceNodeId) {
          sourceNodeId = this.addNode({
            id: this.generateNodeID('source'),
            type: 'source',
            table: column.table,
            inputs: [],
            timeColumn: this.config.tables[column.table]?.timeColumn,
          });
        }

        // Create projection node
        let created = this.addNode({
          id: this.generateNodeID('projection'),
          type: 'projection',
          inputs: [sourceNodeId],
          columns: [
            {
              name: column.column,
              alias:
                expression.alias ||
                (expression.metric !== column.column ? expression.metric : undefined),
              sourceNode: sourceNodeId,
            },
          ],
        });

        if (expression.filter) {
          let filterNodeId = this.processFilter(expression.filter, [created]);
        }

        return created;
      }

      case 'constant': {
        // Constants become parameters
        const paramValue = this.createParameter(expression.value);
        return this.addNode({
          id: this.generateNodeID('expression'),
          type: 'expression',
          inputs: [],
          expression: {
            type: 'constant',
            value: paramValue,
          },
          metadata: {
            isParameter: true,
          },
          alias: expression.alias,
        });
      }

      case 'aggregate': {
        // Process target expression first
        const targetNodeId = this.processExpression(expression.target, true);
        let inputs = [targetNodeId];
        if (expression.filter) {
          // Process filter expression
          const filterNodeId = this.processFilter(expression.filter);
          inputs.push(filterNodeId);
          // Once we have created a filter for the aggregation, we can remove the filter from the expression since it's already in the graph
          delete expression.filter;
        }
        return this.addNode({
          id: this.generateNodeID('aggregation'),
          type: 'expression',
          inputs: inputs,
          expression: expression,
          alias: expression.alias || generateAlias(expression),
        });
      }

      case 'math': {
        // Process all operands
        const operandNodeIds = expression.operands.map((operand) =>
          this.processExpression(operand)
        );

        return this.addNode({
          id: this.generateNodeID('expression'),
          type: 'expression',
          inputs: operandNodeIds,
          expression: expression,
          alias: expression.alias,
        });
      }

      default:
        throw new Error(`Unknown expression type: ${expression['type']}`);
    }
  }

  private ensureRequiredColumns() {
    // Find all source nodes
    const sourceNodes = Object.values(this.nodes).filter(
      (node) => node.type === 'source'
    ) as SourceNode[];

    // Process each source node
    sourceNodes.forEach((sourceNode) => {
      const tableConfig = this.config.tables[sourceNode.table];

      // Get columns that need to be included
      const requiredColumns = new Set<string>();

      // Add alwaysIncludeColumns if they exist
      if (tableConfig.alwaysIncludeColumns?.length) {
        tableConfig.alwaysIncludeColumns.forEach((col) => requiredColumns.add(col));
      }

      // Check if we have any time-based aggregations depending on this source
      const hasTimeBasedAggregation = Object.values(this.nodes).some((node) => {
        if (node.type !== 'expression') return false;
        const expr = node as ExpressionNode;

        // Check if it's an aggregate expression with a time range
        if (expr.expression.type !== 'aggregate' || !expr.expression.time_range) {
          return false;
        }

        // Check if this aggregation depends on our source node
        // We need to traverse the inputs chain up to the source
        let currentNode: ComputeNode | undefined = node;
        while (currentNode) {
          if (currentNode.inputs.includes(sourceNode.id)) {
            return true;
          }
          // Move to the first input node
          currentNode =
            currentNode.inputs.length > 0 ? this.nodes[currentNode.inputs[0]] : undefined;
        }
        return false;
      });

      // If we have time-based aggregations and a timeColumn, add it to required columns
      if (hasTimeBasedAggregation && tableConfig.timeColumn) {
        requiredColumns.add(tableConfig.timeColumn);
      }

      // Find existing projections from this source
      const existingProjections = Object.values(this.nodes).filter(
        (node) => node.type === 'projection' && node.inputs.includes(sourceNode.id)
      ) as ProjectionNode[];

      // Get all currently projected columns from this source
      const projectedColumns = new Set(
        existingProjections.flatMap((node) =>
          node.columns.filter((col) => 'name' in col).map((col) => (col as { name: string }).name)
        )
      );

      // Find which required columns are missing
      const missingColumns = Array.from(requiredColumns).filter(
        (col) => !projectedColumns.has(col)
      );

      if (missingColumns.length > 0) {
        missingColumns.forEach((col) => {
          // Create a new projection node for missing columns
          this.addNode({
            id: this.generateNodeID('projection'),
            type: 'projection',
            inputs: [sourceNode.id],
            columns: [
              {
                name: col,
                sourceNode: sourceNode.id,
              },
            ],
            metadata: {
              isRequiredProjection: true,
            },
          });
        });
      }
    });
  }

  /**
   * Builds a single multi-way join across all source tables using shared PKs,
   * then rewires downstream nodes to reference the join output instead of
   * individual sources.
   */
  inferJoins(): void {
    /* 1. Collect all source nodes actually referenced. */
    const sourceNodes = Object.values(this.nodes).filter(
      (n) => n.type === 'source'
    ) as SourceNode[];

    if (sourceNodes.length <= 1) return; // single-table screener

    /* 2. Build a map tableName -> nodeId */
    const tableToNode: Record<string, SourceNode> = {};
    sourceNodes.forEach((sn) => (tableToNode[sn.table] = sn));

    /* 3. Figure out what tables have to be joined.   (naïve: all of them) */
    const involvedTables = Object.keys(tableToNode);
    const joinConds: JoinCondition[] = [];

    for (let i = 0; i < involvedTables.length - 1; i++) {
      for (let j = i + 1; j < involvedTables.length; j++) {
        const t1 = this.config.tables[involvedTables[i]];
        const t2 = this.config.tables[involvedTables[j]];
        const pk = t1.primaryKeys.find((k) => t2.primaryKeys.includes(k));
        if (!pk) {
          throw new Error(`No common PK between ${t1.name} and ${t2.name}`);
        }
        joinConds.push({
          left: { input: tableToNode[t1.name].id, column: pk },
          right: { input: tableToNode[t2.name].id, column: pk },
          op: '=',
        });
      }
    }

    /* 4. Create the JoinNode */
    const joinNodeId = this.addNode({
      id: this.generateNodeID('join'),
      type: 'join',
      inputs: sourceNodes.map((sn) => sn.id),
      joinType: 'INNER',
      joinConditions: joinConds,
    });

    /* 5. Re-wire every projection / filter / expression that pointed to a
     *source* node so it now points to the join node. */
    sourceNodes.forEach((sn) => this.replaceNodeID(sn.id, joinNodeId));

    /* 6. Optional: mark original source nodes non-terminal so optimiser
                can cull unused ones later (they’re still needed for FROM list
                inside the Join CTE). */
    sourceNodes.forEach((sn) => (sn.isTerminal = false));
  }

  /** VISUALISATION */

  toMermaid(): string {
    // Delegates to standalone renderer for clarity/testing
    return renderMermaid(this);
  }

  public getExpressionLabel(expr: Expression): string {
    if (!expr) return 'unknown';

    const alias = expr.alias ? ` as ${expr.alias}` : '';

    switch (expr.type) {
      case 'metric':
        return `${expr.metric}${alias}`;
      case 'constant':
        return `${this.formatConstantForLabel(expr.value)}${alias}`;
      case 'aggregate':
        return expr.alias
          ? expr.alias
          : `${expr.aggregation}(${this.getExpressionLabel(expr.target)})`;
      case 'math':
        return `(${expr.operands
          .map((op) => (typeof op === 'string' ? op : this.getExpressionLabel(op)))
          .join(` ${expr.operator} `)})${alias}`;
      default:
        return 'expr';
    }
  }

  private getExpressionDependencies(expr: Expression): NodeID[] {
    const deps: NodeID[] = [];

    if (!expr) return deps;

    if ('inputs' in expr && Array.isArray((expr as unknown as { inputs: NodeID[] }).inputs)) {
      deps.push(...(expr as unknown as { inputs: NodeID[] }).inputs);
    }

    if (expr.type === 'math') {
      expr.operands.forEach((op) => {
        if (typeof op === 'string') {
          deps.push(op);
        } else {
          deps.push(...this.getExpressionDependencies(op));
        }
      });
    }

    return [...new Set(deps)];
  }

  private formatConstantForLabel(
    value: string | number | boolean | string[] | number[]
  ): string {
    const stringify = (val: string | number | boolean | string[] | number[]): string => {
      if (Array.isArray(val)) {
        return `[${val.map((v) => (typeof v === 'string' ? `'${v}'` : String(v))).join(', ')}]`;
      }
      if (typeof val === 'string') return `'${val}'`;
      return String(val);
    };

    if (typeof value === 'string') {
      const match = value.match(/^\{(param_\d+):/);
      if (match) {
        const paramValue = this.parameters[match[1]];
        if (paramValue !== undefined) {
          return this.truncateForLabel(stringify(paramValue));
        }
      }
    }

    return this.truncateForLabel(stringify(value));
  }

  private truncateForLabel(value: string, maxLength = 40): string {
    return value.length > maxLength ? `${value.slice(0, maxLength - 3)}...` : value;
  }

  /** OPTIMISATIONS */

  optimize(risky = false): void {
    this.removedDuplicateProjections();
    this.inlineParameters();
    this.mergeFilters();
    this.removeUselessCompositeFilters();
    this.removeDuplicateProjectionExpressions();
    if (risky) this.trySimplification1();
    if (risky) this.removeDuplicateFilters();

    this.ensureRequiredColumns();
  }

  /**
   * Removes useless composite filters
   * These filters have only one input and a 'and' or 'or' operator (which requires at least two inputs)
   * They can be safely removed and their input can be directly connected to their dependent nodes
   */
  private removeUselessCompositeFilters() {
    const compositeFilters = Object.values(this.nodes).filter(
      (node) => node.type === 'composite-filter'
    ) as CompositeFilterNode[];
    for (const node of compositeFilters) {
      if (node.inputs.length === 1) {
        // Replace all references to this node with its input
        const dependents = this.findDependentNodes(node.id);
        for (const dependent of dependents) {
          dependent.inputs = dependent.inputs.map((input) =>
            input === node.id ? node.inputs[0] : input
          );
        }
        this.removeNode(node.id);
      }
    }
  }

  /**
   * Merges adjacent filter nodes with same inputs
   * Particularly useful for composite filters
   */
  private mergeFilters() {
    const nodes = Object.values(this.nodes);

    // First pass: merge simple filters into single composite nodes
    for (const node of nodes) {
      if (node.type !== 'filter' && node.type !== 'composite-filter') continue;

      // Find other filters with same inputs
      const relatedFilters = nodes.filter(
        (n) =>
          (n.type === 'filter' || n.type === 'composite-filter') &&
          n !== node &&
          JSON.stringify(n.inputs.sort()) === JSON.stringify(node.inputs.sort())
      );

      if (relatedFilters.length === 0) continue;

      // Create a composite filter node
      const newNode: CompositeFilterNode = {
        id: this.generateNodeID('composite-filter'),
        type: 'composite-filter',
        inputs: node.inputs,
        operator: 'and',
      };

      // Add new node
      this.addNode(newNode);

      // Remove old nodes
      this.removeNode(node.id);
      relatedFilters.forEach((f) => this.removeNode(f.id));

      // Update references
      const allAffectedIds = [node.id, ...relatedFilters.map((f) => f.id)];
      allAffectedIds.forEach((oldId) => {
        const dependentNodes = this.findDependentNodes(oldId);
        dependentNodes.forEach((depNode) => {
          depNode.inputs = depNode.inputs.map((input) => (input === oldId ? newNode.id : input));
        });
      });
    }
  }

  // Remove duplicate projection nodes
  // Two projection nodes are considered duplicates if they have the same inputs and columns
  // and their dependent nodes are either the same or have the same inputs
  // "Required" projections are not removed
  private removedDuplicateProjections() {
    const projectionNodes = Object.values(this.nodes).filter(
      (node) => node.type === 'projection'
    ) as ProjectionNode[];

    const uniqueProjections: Record<string, ProjectionNode> = {};

    // Process non-required projections first
    const normalProjections = projectionNodes.filter(
      (node) => !node.metadata?.isRequiredProjection
    );
    const requiredProjections = projectionNodes.filter(
      (node) => node.metadata?.isRequiredProjection
    );

    // Process normal projections
    normalProjections.forEach((node) => {
      let key =
        node.inputs.sort().join(',') +
        node.columns
          .sort()
          .map((c) => ('name' in c ? c.name : c.expression))
          .join(',');

      const dependents = this.findDependentNodes(node.id);
      key += dependents.map((d) => d.inputs.sort().join(',')).join(',');

      if (!uniqueProjections[key]) {
        uniqueProjections[key] = node;
      } else {
        // Replace references to old projection
        this.replaceNodeID(node.id, uniqueProjections[key].id);
        this.removeNode(node.id);
      }
    });

    // Keep required projections separate to ensure they're not removed
    requiredProjections.forEach((node) => {
      const key = `required_${node.inputs.sort().join(',')}_${node.columns
        .sort()
        .map((c) => ('name' in c ? c.name : c.expression))
        .join(',')}`;

      if (!uniqueProjections[key]) {
        uniqueProjections[key] = node;
      } else {
        // Only replace with another required projection
        this.replaceNodeID(node.id, uniqueProjections[key].id);
        this.removeNode(node.id);
      }
    });
  }

  private removeDuplicateProjectionExpressions() {
    // Remove duplicate [source/join -> projection -> expression -> (only 1 dependent)] groups
    // This can happen when the same expression is applied to the same column multiple times

    const expressions = (
      Object.values(this.nodes).filter((node) => node.type === 'expression') as ExpressionNode[]
    )
      .filter((node) => node.inputs.length === 1) // Only consider expressions with a single input
      .filter((node) => this.findDependentNodes(node.id).length === 1) // Only consider expressions with a single dependent
      .filter((node) => {
        const parent = this.nodes[node.inputs[0]];
        return parent.type === 'projection';
      }); // Only consider expressions with a projection as dependent
    const uniqueExpressions: ExpressionNode[] = [];
    for (const expr of expressions) {
      if (!uniqueExpressions.some((e) => areExpressionsEqual(e.expression, expr.expression))) {
        uniqueExpressions.push(expr);
      } else {
        const previousExpr = uniqueExpressions.find((e) =>
          areExpressionsEqual(e.expression, expr.expression)
        )!;
        this.replaceNodeID(expr.id, previousExpr.id);
        this.removeNode(expr.id);
        this.removeNode(expr.inputs[0]);
      }
    }
  }

  /**
   * Try to simply the graph when a join is involved on another table
   * to only retrieve a column present on both tables.
   * For example, if we filter only on 'ticker' for a function in table 'daily_agg'
   * we shouldn't need to join the table 'tickers' since 'ticker' exists in both tables
   */
  private trySimplification1(): void {
    const sourceNodes = Object.values(this.nodes).filter(
      (node) => node.type === 'source'
    ) as SourceNode[];
    if (sourceNodes.length !== 2) return; // TODO: if more tables in database, we need to update this logic

    // To detect when we can run this simplication, we will look for when the source node
    // is table 'tickers', all its projections are only for column 'ticker'
    // and the filters on these projections are only on 'ticker'
    const tickerSource = sourceNodes.find((node) => node.table === 'tickers');
    if (!tickerSource) return;
    const deps = this.findDependentNodes(tickerSource.id);

    // Check if all projections are only for column 'ticker'
    const projections = deps.filter((node) => node.type === 'projection');
    if (deps.length !== projections.length) return;
    if (!projections.every((node) => (node as ProjectionNode).columns.length === 1)) return;
    if (
      !projections.every((node) => {
        const col = (node as ProjectionNode).columns[0];
        return 'name' in col && col.name === 'ticker';
      })
    )
      return;

    // Check if all filters depending on those projections are only on 'ticker'
    const deps2 = deps.flatMap((node) => this.findDependentNodes(node.id));
    const filters = deps2.filter((node) => node.type === 'filter');
    if (deps2.length !== filters.length) return;
    if (
      !filters.every((node) => {
        const filterNode = node as FilterNode;
        const leftMetric =
          'metric' in filterNode.condition.left ? filterNode.condition.left.metric : undefined;
        const rightMetric =
          'metric' in filterNode.condition.right ? filterNode.condition.right.metric : undefined;
        return leftMetric === 'ticker' || rightMetric === 'ticker';
      })
    )
      return;

    // If we reach this point, we can remove the source, the ticker projections and the filters
    // Add replace them with a projection + filter on the other table
    const otherSource = sourceNodes.find((node) => node.table === 'daily_agg');
    if (!otherSource) return;
    const newProjectionId = this.addNode({
      id: this.generateNodeID('projection'),
      type: 'projection',
      inputs: [otherSource.id],
      columns: [
        {
          name: 'ticker',
          sourceNode: otherSource.id,
        },
      ],
    });

    // Replace filters' projections that are 'ticker' with the new projection
    filters.forEach((node) => {
      node.inputs = [newProjectionId];
    });

    // Remove old nodes
    this.removeNode(tickerSource.id);
    projections.forEach((node) => this.removeNode(node.id));
  }

  /**
   * Duplicate filters are filters that have the same projection as input (or semantically the same)
   * and the exact same condition. We can remove one of them and update the references to the other
   */
  private removeDuplicateFilters() {
    let filters = Object.values(this.nodes)
      .filter((node) => node.type === 'filter')
      .filter((node) => {
        // All the inputs are projections
        return node.inputs.every((input) => {
          const inputNode = this.nodes[input];
          return inputNode.type === 'projection';
        });
      });
    for (const filter of filters) {
      // Find filters that have the same input and the same condition
      const alreadyExistingSameFilters = filters.filter((node) => {
        if (node.id === filter.id) return false;
        if (node.inputs.length !== filter.inputs.length) return false;
        if (node.inputs.some((input, index) => filter.inputs[index] !== input)) return false;
        return JSON.stringify(node.metadata) === JSON.stringify(filter.metadata);
      });
      if (alreadyExistingSameFilters.length > 0) {
        // Replace the current filter with the first one
        this.replaceNodeID(filter.id, alreadyExistingSameFilters[0].id);
        this.removeNode(filter.id);
        // Remove from the current filters loop
        filters = filters.filter((f) => f.id !== filter.id);
      }
    }
  }

  /**
   * Inline expressions that are parameters
   */
  private inlineParameters(): void {
    const expressionNodes = Object.values(this.nodes).filter(
      (node) => node.type === 'expression'
    ) as ExpressionNode[];
    for (const node of expressionNodes) {
      if (!node.metadata?.isParameter) continue;
      const constantNode = node.expression as ConstantExpression;
      const dependentNodes = this.findDependentNodes(node.id);
      for (const dependent of dependentNodes) {
        if ('condition' in dependent) {
          let left = dependent.condition.left;
          let right = dependent.condition.right;

          if ('input' in left)
            left = left.input === node.id ? { parameter: constantNode.value as string } : left;
          if ('input' in right)
            right = right.input === node.id ? { parameter: constantNode.value as string } : left;

          dependent.condition = {
            left,
            right,
            op: dependent.condition.op,
          };
          dependent.metadata = { ...dependent.metadata, hasParameter: true };
          dependent.inputs = dependent.inputs.filter((id) => id !== node.id);
        }
        if ('criteria' in dependent) {
          dependent.criteria = dependent.criteria.map((crit) => {
            if (crit.expression === node.id) crit.expression = constantNode.value as string;
            return crit;
          });
          dependent.metadata = { ...dependent.metadata, hasParameter: true };
          dependent.inputs = dependent.inputs.filter((id) => id !== node.id);
        }
      }
      this.removeNode(node.id);
    }
  }

  /**
   * Helper to find all nodes that depend on a given node
   */
  private findDependentNodes(nodeId: NodeID): ComputeNode[] {
    return Object.values(this.nodes).filter((node) => node.inputs.includes(nodeId));
  }

  /**
   *  Helper to replace a node ID in the compute graph by another node ID
   */
  private replaceNodeID(oldId: NodeID, newId: NodeID, oldAlias?: string): void {
    const nodes = Object.values(this.nodes);
    for (const node of nodes) {
      if (node.id === newId) continue;
      node.inputs = node.inputs.map((input) => (input === oldId ? newId : input));
      if ('condition' in node && node.condition) {
        let left = node.condition.left;
        let right = node.condition.right;

        if ('input' in left && left.input === oldId) {
          const tmp: string | undefined = oldAlias;
          left = { input: newId, metric: tmp ?? ('metric' in left ? left.metric : newId) };
        }
        if ('input' in right && right.input === oldId) {
          const tmp: string | undefined = oldAlias;
          right = { input: newId, metric: tmp ?? ('metric' in right ? right.metric : newId) };
        }

        node.condition = {
          left,
          right,
          op: node.condition.op,
        };
      }
      if ('criteria' in node) {
        node.criteria = node.criteria.map((crit) => {
          if (crit.expression === oldId) crit.expression = newId;
          return crit;
        });
      }
      if ('columns' in node) {
        node.columns = node.columns?.map((col) => {
          if ('sourceNode' in col && col.sourceNode === oldId) col.sourceNode = newId;
          return col;
        });
      }
    }
  }
}
