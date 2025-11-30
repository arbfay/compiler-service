import { ComputeGraph, generateAlias } from '..';
import { DEFAULT_CONFIG } from '../../settings';

interface ExpressionSQL {
  column: string;
  where?: string[];
  having?: string[];

  /* QUALIFY clauses (for window functions) */
  qualify?: string[];

  group_by?: string[];
  order_by?: string[];
  isWindow?: boolean;
}

export class QueryBuilder {
  // Translate a ComputeGraph to a Clickhouse SQL query
  private context: TranslationContext;
  private sql: string;
  private cols: string[] = [];
  private fromStmt: string = '';
  private preFilters: string[] = [];
  private filters: string[] = [];
  private qualify: string[] = [];
  private groupBy: string[] = [];
  private orderBy: string[] = [];
  private limitSql: string = '';
  private usedAliases: string[] = [];

  constructor(context: TranslationContext) {
    this.context = context;
    this.sql = '';
  }

  static fromComputeGraph(computeGraph: ComputeGraph, config = DEFAULT_CONFIG): QueryBuilder {
    const nodes = computeGraph.getExecutionOrder();
    // Initialize context with nodes and visited set
    const context: TranslationContext = {
      config,
      nodes: nodes.reduce((acc, node) => ({ ...acc, [node.id]: node }), {}),
      visited: new Set<NodeID>(),
      timeRanges: nodes
        .map((node) =>
          node.type === 'expression' && node.expression.type === 'aggregate'
            ? node.expression.time_range
            : undefined
        )
        .filter(Boolean) as TimeRange[], // Get all time ranges in the graph
    };
    return new QueryBuilder(context);
  }

  public translateToSQL(instructions?: { limit_by_one_ticker?: boolean }): string {
    // Find groups of nodes that should be CTEs
    let nodeGroups = this.groupNodesForCTE();

    // Remove CTE nodes that depends only on one other CTE node (which means it won't be used)
    // Replace their reference with the dependent node
    nodeGroups = this.removeUnusedCTENodes(nodeGroups);

    // Generate base CTE SQL for each group
    const cteSqls = nodeGroups
      .map((group) => this.generateCTESQL(group))
      .filter((sql) => sql !== undefined) as { id: string; sql: string; columns: string[] }[];

    // Get all the nodes that are not in a CTE group
    const nonCteNodes = Object.values(this.context.nodes).filter(
      (node) => !nodeGroups.some((group) => group.nodes.includes(node.id))
    );
    this.buildMainQuery(cteSqls, nonCteNodes, nodeGroups);

    return this.assembleSQL();
  }

  private buildMainQuery(
    cteSqls: { id: string; sql: string; columns: string[] }[],
    nonCteNodes: ComputeNode[],
    nodeGroups: NodeGroup[]
  ): void {
    // Build WITH clause if we have CTEs
    if (cteSqls.length > 0) {
      this.sql = `WITH ${cteSqls.map((c) => c.sql).join(',\n')}\n`;
    }

    // Add columns from CTEs
    this.addCTEColumns(cteSqls);

    // Build FROM clause
    this.buildFromClause(cteSqls);

    // Add filters
    this.addFilters(nonCteNodes, nodeGroups);

    // Add ORDER BY
    this.addOrderBy(nonCteNodes);

    // Add LIMIT
    this.addLimit(nonCteNodes);
  }

  private addCTEColumns(cteSqls: { id: string; sql: string; columns: string[] }[]): void {
    cteSqls.forEach((c) => {
      for (const col of c.columns) {
        if (!this.usedAliases.includes(col)) {
          this.usedAliases.push(col);
          this.cols.push(`${c.id}.${col} AS ${col}`);
        } else {
          this.cols.push(`${c.id}.${col}`);
        }
      }
    });
  }

  private buildFromClause(cteSqls: { id: string; sql: string; columns: string[] }[]): void {
    if (cteSqls.length > 0) {
      this.fromStmt = ` FROM ${cteSqls[0].id}`;
      // Join remaining CTEs
      for (let i = 1; i < cteSqls.length; i++) {
        this.fromStmt += ` CROSS JOIN ${cteSqls[i].id}`;
      }
    }
  }

  /** Add top-level filters that are NOT inside a CTE. */
  private addFilters(nonCteNodes: ComputeNode[], nodeGroups: NodeGroup[]): void {
    const filterNodes = nonCteNodes.filter(
      (n) => n.type === 'filter' || n.type === 'composite-filter'
    );

    if (filterNodes.length === 0) return;

    /* Top-level window aliases present in *this* SELECT scope */
    const windowAliases = new Set<string>();

    /* 1. gather aliases from stand-alone expression nodes */
    nonCteNodes.forEach((n) => {
      if (n.type !== 'expression') return;
      const ex = (n as ExpressionNode).expression;
      if (ex.type === 'aggregate' && ex.time_range) {
        const alias = (n as ExpressionNode).alias ?? generateAlias(ex)!;
        (n as ExpressionNode).alias = alias;
        windowAliases.add(alias);
      }
    });

    /* 2.  gather aliases from projection columns */
    nonCteNodes
      .filter((n) => n.type === 'projection')
      .forEach((pid) => {
        const proj = this.context.nodes[pid.id] as ProjectionNode;
        proj.columns.forEach((col) => {
          if (
            'expression' in col &&
            col.expression.type === 'aggregate' &&
            col.expression.time_range
          ) {
            if (!col.alias) col.alias = generateAlias(col.expression)!;
            windowAliases.add(col.alias);
          }
        });
      });

    /* 3. process each Filter / Composite-Filter node */
    const alreadyApplied = new Set(
      nodeGroups.flatMap((g) =>
        g.nodes.filter((id) => id.startsWith('filter_') || id.startsWith('composite_filter_'))
      )
    );

    for (const f of filterNodes) {
      if (alreadyApplied.has(f.id)) continue;

      const { sql, touchesWindow } = translateFilter(
        f as FilterNode,
        { ...this.context, cteGroups: nodeGroups },
        [],
        windowAliases
      );

      if (!sql) continue;

      if (touchesWindow) this.qualify.push(sql);
      else this.filters.push(sql);
    }
  }

  private addOrderBy(nonCteNodes: ComputeNode[]): void {
    const orderBys = nonCteNodes.filter((node) => node.type === 'sort');
    if (orderBys.length > 0) {
      this.orderBy = orderBys.map((node) => this.translateSortNode(node as SortNode));
    }
  }

  private translateSortNode(node: SortNode): string {
    return node.criteria
      .map((c) => {
        const maybeNode = this.context.nodes[c.expression];
        let expr = c.expression;
        if (maybeNode && 'alias' in maybeNode && maybeNode.alias) {
          expr = maybeNode.alias;
        }
        return `${expr} ${c.direction}`;
      })
      .join(', ');
  }

  private addLimit(nonCteNodes: ComputeNode[]): void {
    const limitNode = nonCteNodes.find((node) => node.type === 'limit') as LimitNode;
    if (limitNode) {
      this.limitSql = `LIMIT ${limitNode.limit}`;
      if (limitNode.metadata?.offset) {
        this.limitSql += ` OFFSET ${limitNode.metadata.offset}`;
      }
      if (limitNode.metadata?.isGrouped && limitNode.metadata.groupDimension) {
        this.groupBy.push(limitNode.metadata.groupDimension);
        this.limitSql += ` BY ${limitNode.metadata.groupDimension}`;
      }
    }
  }

  private assembleSQL(): string {
    const parts: string[] = [];

    // SELECT
    parts.push(`SELECT ${this.cols.join(', ')}`);

    // FROM
    parts.push(this.fromStmt);

    // PREWHERE
    if (this.preFilters.length > 0) {
      parts.push(`PREWHERE ${this.preFilters.join(' AND ')}`);
    }

    // WHERE
    if (this.filters.length > 0) {
      parts.push(`WHERE ${this.filters.join(' AND ')}`);
    }

    // QUALIFY
    if (this.qualify.length > 0) {
      parts.push(`QUALIFY ${this.qualify.join(' AND ')}`);
    }

    // GROUP BY
    if (this.groupBy.length > 0) {
      const uniqueGroups = [...new Set(this.groupBy)];
      parts.push(`GROUP BY ${uniqueGroups.join(', ')}`);
    }

    // ORDER BY
    if (this.orderBy.length > 0) {
      parts.push(`ORDER BY ${this.orderBy.join(', ')}`);
    }

    // LIMIT
    if (this.limitSql) {
      parts.push(this.limitSql);
    }

    return this.sql + parts.join('\n');
  }

  private findDependentNodes(
    nodeId: NodeID,
    nodes: ComputeNode[] = Object.values(this.context.nodes)
  ): ComputeNode[] {
    return nodes.filter((node) => node.inputs.includes(nodeId));
  }

  private groupNodesForCTE(): NodeGroup[] {
    const groups: NodeGroup[] = [];
    const nodes = Object.values(this.context.nodes);

    // Start with nodes that should be CTEs
    const cteNodes = nodes.filter((node) => this.shouldBeCTE(node));

    for (const cteNode of cteNodes) {
      const group: NodeGroup = {
        id: `cte_${groups.length}`,
        nodes: [...cteNode.inputs, cteNode.id],
      };

      // Add all nodes that only depend on this CTE node
      const dependentNodes = nodes
        .filter((n) => n.inputs.includes(cteNode.id) && n.inputs.length === 1)
        // Sort nodes will not be in CTEs
        .filter((n) => n.type !== 'sort')
        // Metric nodes will not be in CTEs
        .filter((n) => n.type !== 'expression' || n.expression?.type === 'metric')
        // Filter nodes will not be in CTEs
        .filter((n) => n.type !== 'filter');
      group.nodes.push(...dependentNodes.map((n) => n.id));

      // Add projection nodes
      const projectionNodes = nodes.filter(
        (n) => n.type === 'projection' && n.inputs.includes(cteNode.id)
      );
      group.nodes.push(...projectionNodes.map((n) => n.id));

      // Add "last" type nodes
      const lastNodes = nodes.filter(
        (n) =>
          n.type === 'expression' &&
          n.expression?.type === 'aggregate' &&
          n.expression.aggregation === 'last' &&
          n.inputs.every((id) => projectionNodes.map((p) => p.id).includes(id))
      );
      group.nodes.push(...lastNodes.map((n) => n.id));

      // Add filter nodes
      this.addFilterNodesToGroup(group, nodes, projectionNodes, lastNodes);

      // Handle existing groups
      this.handleExistingGroups(group, groups);

      groups.push(group);
    }

    return groups;
  }

  private addFilterNodesToGroup(
    group: NodeGroup,
    nodes: ComputeNode[],
    projectionNodes: ComputeNode[],
    lastNodes: ComputeNode[]
  ): void {
    // Filter nodes that are only used in projection nodes
    const filterNodes1 = nodes
      .filter(
        (n) =>
          n.type === 'filter' &&
          n.inputs.every((id) => projectionNodes.map((p) => p.id).includes(id))
      )
      .filter((n) =>
        this.findDependentNodes(n.id, nodes).every(
          (d) => d.type !== 'expression' || d.expression?.type !== 'aggregate'
        )
      );
    // Filter nodes that are only used in last nodes
    const filterNodes2 = nodes.filter(
      (n) => n.type === 'filter' && n.inputs.every((id) => lastNodes.map((p) => p.id).includes(id))
    );

    group.nodes.push(...filterNodes1.map((f) => f.id));
    group.nodes.push(...filterNodes2.map((f) => f.id));
  }

  private handleExistingGroups(group: NodeGroup, groups: NodeGroup[]): void {
    // Remove nodes already in another group and replace with group ID
    for (const n of [...group.nodes]) {
      for (const existingGroup of groups) {
        if (existingGroup.nodes.includes(n)) {
          group.nodes = group.nodes.filter((id) => id !== n);
          group.nodes.push(existingGroup.id);
        }
      }
    }

    // Remove duplicates
    group.nodes = [...new Set(group.nodes)];
  }

  private shouldExpressionBeCTE(node: ExpressionNode): boolean {
    const nodes = Object.values(this.context.nodes);

    // Check for multiple dependents
    if (nodes.filter((n) => n.inputs.includes(node.id)).length > 1) {
      return true;
    }

    // Check for multiple inputs
    if (node.inputs.length > 1) {
      return true;
    }

    // Check time-based calculations
    if ('time_range' in node.expression) {
      const hasTimeCalc = node.expression?.time_range;
      if (!hasTimeCalc) return false;

      const timeRanges = new Set(
        nodes
          .filter(
            (n) =>
              n.type === 'expression' &&
              n.expression.type === 'aggregate' &&
              n.expression.time_range
          )
          .filter(Boolean)
          .map((n) => {
            const expr = (n as ExpressionNode).expression as AggregateExpression;
            return JSON.stringify(expr.time_range);
          })
      );

      return timeRanges.size > 1 || node.expression.time_range?.type === 'relative';
    }

    return false;
  }

  private shouldBeCTE(node: ComputeNode): boolean {
    const nodes = Object.values(this.context.nodes);
    const multipleReferences = nodes.filter((n) => n.inputs.includes(node.id)).length > 1;

    if (node.type === 'filter') {
      return this.shouldFilterBeCTE(node);
    }

    if (node.type === 'expression') {
      return this.shouldExpressionBeCTE(node);
    }

    return multipleReferences;
  }

  private shouldFilterBeCTE(node: FilterNode): boolean {
    const nodes = Object.values(this.context.nodes);
    if (node.inputs.length === 1) {
      const uniqueParent = nodes.find((n) => n.inputs.includes(node.id));
      return uniqueParent?.type === 'projection';
    }
    return false;
  }

  private removeUnusedCTENodes(groups: NodeGroup[]): NodeGroup[] {
    let finalGroups = [];
    for (let g of groups) {
      let cteNodes = g.nodes.filter((id) => id.startsWith('cte_'));
      if (!cteNodes || !cteNodes.length) {
        finalGroups.push(g);
      } else if (g.nodes.length === 1) {
        // It means this cte does nothing else but get the column of another CTE
        groups.map((group) => {
          if (group.nodes.includes(g.id)) {
            // Replace with cteNodes[0]
            group.nodes = group.nodes.map((id) => (id === g.id ? cteNodes[0] : id));
          }
        });
      } else {
        finalGroups.push(g);
      }
    }
    return finalGroups;
  }

  /** Build the CTE that corresponds to one NodeGroup */
  /**
   * Build a Common-Table-Expression for one NodeGroup.
   * Decides WHERE vs QUALIFY by inspecting the predicate itself
   * (alias match or inline window expression).
   */
  private generateCTESQL(
    group: NodeGroup
  ): { id: string; sql: string; columns: string[] } | undefined {
    /* ──────────────────────────────── setup ───────────────────────────── */

    let fromStmt = 'FROM ';
    let cols: string[] = [];
    let where: string[] = [];
    let qualify: string[] = [];
    let preWhere: string[] = [];
    let groupBy: string[] = [];
    let orderBy: string[] = [];

    const cteIds = group.nodes.filter((id) => id.startsWith('cte_'));
    const nonCte = group.nodes.filter((id) => !cteIds.includes(id));
    const sourceIds = nonCte.filter((id) => this.context.nodes[id].type === 'source');

    if (sourceIds.length === 0 && cteIds.length === 0) return;

    const srcCfgs = sourceIds.map((id) => {
      const s = this.context.nodes[id] as SourceNode;
      return this.context.config.tables[s.table];
    });

    /* ──────────── helper: PREWHERE by largest time window ──────────── */

    const addLargestTimeRangeFilter = () => {
      if (!srcCfgs.length) return; // CTE-only group
      const ltr = chooseLargestTimeRange(this.context.timeRanges || []);
      if (!ltr) return;
      const tc = `${srcCfgs[0].name}.${srcCfgs[0].timeColumn}`;

      if (ltr.type === 'relative') {
        preWhere.push(
          `${tc} >= toDate(date_sub(now(), INTERVAL ${ltr.duration} ${ltr.unit.toUpperCase()}))`
        );
      } else if (ltr.type === 'absolute') {
        // From is extended by 1 day to be inclusive
        const from = new Date(ltr.from * 1000 - 86400).toISOString().slice(0, 10);
        const to = new Date(ltr.to * 1000).toISOString().slice(0, 10);
        preWhere.push(`${tc} BETWEEN toDate('${from}') AND toDate('${to}')`);
      }
    };

    /* ──────────── build FROM / JOIN ──────────────────────────── */

    if (srcCfgs.length > 1) {
      const joinTables = (tabs: TableConfig[], srcs: NodeID[], ctes: NodeID[]) => {
        let stmt = '';
        for (let i = 1; i < tabs.length; i++) {
          const t1 = tabs[i - 1],
            t2 = tabs[i];
          const pk = t1.primaryKeys.find((k) => t2.primaryKeys.includes(k));
          if (!pk) throw new Error(`No PK overlap ${t1.name}/${t2.name}`);
          stmt += ` INNER JOIN ${t2.name} ON ${t1.name}.${pk} = ${t2.name}.${pk}`;
        }
        for (const ct of ctes) {
          const proj = this.context.nodes[ct] as ProjectionNode;
          const dep = proj.inputs.find((i) => srcs.includes(i))!;
          const tbl = this.context.config.tables[(this.context.nodes[dep] as SourceNode).table];
          const pk2 = tbl.primaryKeys.find((k) =>
            proj.columns.some((c) => 'name' in c && c.name === k)
          );
          if (!pk2) throw new Error(`No PK to join CTE ${ct}`);
          stmt += ` INNER JOIN ${ct} ON ${tbl.name}.${pk2} = ${ct}.${pk2}`;
        }
        return stmt;
      };

      fromStmt += srcCfgs[0].name + joinTables(srcCfgs, sourceIds, cteIds);
      addLargestTimeRangeFilter();
    } else if (srcCfgs.length === 1) {
      fromStmt += srcCfgs[0].name;
      addLargestTimeRangeFilter();
    } else {
      fromStmt += cteIds[0]; // only CTEs
    }

    if (cteIds.length) {
      const rest = srcCfgs.length === 0 ? cteIds.slice(1) : cteIds;
      if (rest.length) fromStmt += ` ${rest.join(', ')}`;
    }

    /* ──────── identify projection & expression nodes ────────── */

    const projIds = nonCte.filter((id) => this.context.nodes[id].type === 'projection');
    const exprIds = nonCte.filter((id) => this.context.nodes[id].type === 'expression');

    /* ─── STEP 1: collect all window-function aliases in this scope ─── */

    const windowAliases = new Set<string>();
    const remember = (alias: string) => {
      if (alias) windowAliases.add(alias);
    };

    exprIds.forEach((id) => {
      const n = this.context.nodes[id] as ExpressionNode;
      if (n.expression.type === 'aggregate' && n.expression.time_range) {
        if (!n.alias) n.alias = generateAlias(n.expression)!;
        remember(n.alias);
      }
    });

    projIds.forEach((id) => {
      const p = this.context.nodes[id] as ProjectionNode;
      p.columns.forEach((col) => {
        if (
          'expression' in col &&
          col.expression.type === 'aggregate' &&
          col.expression.time_range
        ) {
          if (!col.alias) col.alias = generateAlias(col.expression)!;
          remember(col.alias);
        }
      });
    });

    /* ─── helper: decide WHERE vs QUALIFY ───────────────────────── */

    const inlineWindowRegex = /\b(last_value|first_value|avg|sum|min|max|count)\s*\(/i;
    const pushPredicate = (node: ComputeNode, sql: string) => {
      if (!sql) return;
      const touchesWindow =
        [...windowAliases].some((a) => new RegExp(`\\b${a}\\b`).test(sql)) ||
        inlineWindowRegex.test(sql);
      (touchesWindow ? qualify : where).push(sql);
    };

    /* ─── STEP 2: emit SELECT columns ───────────────────────────────── */

    if (!projIds.length && !exprIds.length) {
      cols.push('*');
    }

    for (const pid of projIds) {
      const p = this.context.nodes[pid] as ProjectionNode;
      for (const c of p.columns) {
        if ('name' in c) {
          cols.push(c.alias ? `${c.name} AS ${c.alias}` : c.name);
        } else {
          const out = translateExpression({ ...c.expression, alias: c.alias });
          out.where?.forEach((w) => pushPredicate(p, w));
          cols.push(`${out.column} AS ${c.alias}`);
        }
      }
    }

    for (const eid of exprIds) {
      const e = this.context.nodes[eid] as ExpressionNode;
      const out = translateExpression({ ...e.expression, alias: e.alias });
      out.where?.forEach((w) => pushPredicate(e, w));
      cols.push(out.column);
    }

    /* ─── STEP 3: explicit FILTER nodes ───────────────────────────────── */

    const filterIds = nonCte.filter((id) => this.context.nodes[id].type === 'filter');
    const seen = new Set<string>();

    for (const fid of filterIds) {
      const { sql, touchesWindow } = translateFilter(
        this.context.nodes[fid] as FilterNode,
        this.context,
        [],
        windowAliases
      );
      if (sql && !seen.has(sql)) {
        seen.add(sql);
        (touchesWindow ? qualify : where).push(sql);
      }
    }

    /* ─── STEP 4: LIMIT / GROUP BY ───────────────────────────────────── */

    const limId = nonCte.find((id) => this.context.nodes[id].type === 'limit');
    let limitClause = '';

    if (limId) {
      const limitNode = this.context.nodes[limId] as LimitNode;
      limitClause = `LIMIT ${limitNode.limit}`;
      if (limitNode.metadata?.offset) limitClause += ` OFFSET ${limitNode.metadata.offset}`;
      if (limitNode.metadata?.isGrouped && limitNode.metadata.groupDimension) {
        groupBy.push(limitNode.metadata.groupDimension);
        limitClause += ` BY ${limitNode.metadata.groupDimension}`;
      }
    }

    cols = Array.from(new Set(cols));
    groupBy = Array.from(new Set(groupBy));

    /* ─── assemble the final CTE SQL ──────────────────────────────── */

    let sql = `SELECT ${cols.join(', ')}\n${fromStmt}`;
    if (preWhere.length) sql += `\nPREWHERE ${preWhere.join(' AND ')}`;
    if (qualify.length) sql += `\nQUALIFY ${qualify.join(' AND ')}`;
    if (where.length) sql += `\nWHERE ${where.join(' AND ')}`;
    if (groupBy.length) sql += `\nGROUP BY ${groupBy.join(', ')}`;
    if (orderBy.length) sql += `\nORDER BY ${orderBy.join(', ')}`;
    if (limitClause) sql += `\n${limitClause}`;

    return {
      id: group.id,
      columns: cols.map((c) => {
        const m = c.match(/^(.*?)(?: AS ([^,]+))?$/);
        return m ? m[2] || m[1] : c;
      }),
      sql: `${group.id} AS (\n${sql}\n)`,
    };
  }
}

function translateExpression(expression: Expression, useAliasOnly = false): ExpressionSQL {
  const aliasSuffix = expression.alias ? ` AS ${expression.alias}` : '';
  let where: string[] = [];

  switch (expression.type) {
    case 'constant':
      return { column: `${expression.value}` };

    case 'metric':
      if (useAliasOnly) {
        // if an alias exists, use it; otherwise fall back to metric name
        return { column: expression.alias || expression.metric };
      }
      return {
        column: `${expression.metric}${aliasSuffix}`,
      };

    case 'math':
      // if alias-only mode and we have an alias, just return it
      if (useAliasOnly && expression.alias) {
        return { column: expression.alias };
      }
      // otherwise translate each operand, collecting any WHERE clauses
      const operands = expression.operands
        .map((op) => {
          const expr = translateExpression(op);
          if (expr.where) where.push(...expr.where);
          return expr.column;
        })
        .join(` ${expression.operator} `);
      return {
        column: `(${operands})${aliasSuffix}`,
        where,
      };

    case 'aggregate':
      // 1) ensure every aggregate has a stable alias
      if (!expression.alias) {
        expression.alias = generateAlias(expression);
      }
      // 2) translate the inner target to gather its WHERE clauses
      const targetExpr = translateExpression(expression.target);
      where = [...(targetExpr.where || [])];

      // 3) push date filters into WHERE
      const tr = expression.time_range;
      if (tr) {
        if (tr.type === 'relative') {
          where.push(`date >= date_sub(now(), INTERVAL ${tr.duration} ${tr.unit.toUpperCase()})`);
        } else if (tr.type === 'absolute') {
          const from = new Date(tr.from * 1000).toISOString().slice(0, 10);
          const to = new Date(tr.to * 1000).toISOString().slice(0, 10);
          where.push(`date BETWEEN toDate('${from}') AND toDate('${to}')`);
        }
      }

      // 4) build the actual aggregation SQL
      const agg = aggregationToSQL(expression, targetExpr.column);
      return {
        column: `${agg.sql} AS ${expression.alias}`,
        where,
        isWindow: !!expression.time_range,
      };
  }
}

function aggregationToSQL(
  expression: AggregateExpression,
  altTarget?: string,
  config = DEFAULT_CONFIG
): { sql: string; qualify: string[] } {
  if (!altTarget && expression.target.type !== 'metric') {
    throw new Error('Invalid target for aggregation');
  }
  const column =
    expression.target.type === 'metric'
      ? config.columnMappings[expression.target.metric]
      : undefined;
  const table = column ? config.tables[column.table] : undefined;
  const timeColumn = table?.timeColumn;
  let qualifyStmt: string[] = [];
  // TODO: we should partition by a certain group key if the aggregation is grouped
  let partition = table ? `PARTITION BY ${table.primaryKeys[0]} ORDER BY ${timeColumn}` : undefined;

  const timeRange = expression.time_range;
  if (timeRange && timeRange.type === 'trading' && partition) {
    const precedingRows = timeRange.duration - 1;
    partition += ` ROWS BETWEEN ${precedingRows} PRECEDING AND CURRENT ROW`;
  }
  if (partition) partition = `OVER (${partition})`;
  const col = altTarget || column?.column;
  const sql = (() => {
    switch (expression.aggregation) {
      case 'first':
        let tmp1 =
          table && timeRange?.type !== 'trading'
            ? `OVER (PARTITION BY ${table.primaryKeys[0]} ORDER BY ${timeColumn} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
            : partition;
        return `first_value(${col}) ${tmp1}`;
      case 'last':
        let tmp12 =
          table && timeRange?.type !== 'trading'
            ? `OVER (PARTITION BY ${table.primaryKeys[0]} ORDER BY ${timeColumn} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
            : partition;
        return `last_value(${col}) ${tmp12}`;
      case 'avg':
        return `avg(${col}) ${partition}`;
      case 'sum':
        return `sum(${col}) ${partition}`;
      case 'min':
        return `min(${col}) ${partition}`;
      case 'max':
        return `max(${col}) ${partition}`;
      case 'median':
        return `quantile(0.5)(${col}) ${partition}`;
      case 'stddev':
        return `stddevPopStable(${col}) ${partition}`;
      case 'variance':
        return `varPop(${col}) ${partition}`;
      case 'count':
        return `count(${col}) ${partition}`;
      case 'diff':
        let tmp2 =
          table && timeRange?.type !== 'trading'
            ? `OVER (PARTITION BY ${table.primaryKeys[0]} ORDER BY ${timeColumn} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
            : partition;
        return `last_value(${col}) ${tmp2} - first_value(${col}) ${tmp2}`;
      case 'diff_pct':
        let tmp3 =
          table && timeRange?.type !== 'trading'
            ? `OVER (PARTITION BY ${table.primaryKeys[0]} ORDER BY ${timeColumn} ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
            : partition;
        return `(last_value(${col}) ${tmp3} - first_value(${col}) ${tmp3}) / nullIf(first_value(${col}) ${tmp3}, 0) * 100`;
      default:
        throw new Error(`Unsupported aggregation type: ${expression.aggregation}`);
    }
  })();

  return { sql, qualify: qualifyStmt };
}

/**
 * Recursively translate filter nodes (simple and composite) into SQL where/qualify
 * clauses while tracking whether any windowed expressions are referenced.
 */
function translateFilter(
  filter: FilterNode | CompositeFilterNode,
  ctx: TranslationContext,
  ignoreIds: string[] = [],
  winAliases: Set<string> = new Set()
): { sql: string; touchesWindow: boolean } {
  if (filter.type === 'composite-filter') {
    // Filter out ignored inputs
    const childIds = filter.inputs.filter((id) => !ignoreIds.includes(id));
    const parts: string[] = [];
    let anyWindow = false;

    for (const cid of childIds) {
      const childNode = ctx.nodes[cid] as FilterNode | CompositeFilterNode;
      const { sql: childSql, touchesWindow: childWin } = translateFilter(
        childNode,
        ctx,
        ignoreIds,
        winAliases
      );
      if (!childSql) continue;
      parts.push(childSql);
      anyWindow = anyWindow || childWin;
    }

    if (parts.length === 0) {
      return { sql: '', touchesWindow: false };
    }
    if (filter.operator === 'not') {
      // NOT only ever has one child
      return { sql: `NOT (${parts[0]})`, touchesWindow: anyWindow };
    }
    // AND / OR join
    const joint = parts.join(` ${filter.operator.toUpperCase()} `);
    return { sql: `(${joint})`, touchesWindow: anyWindow };
  }

  const cond = filter.condition;
  const sideInfo = (side: FilterNode['condition']['left']): { txt: string; win: boolean } => {
    if ('input' in side) {
      const n = ctx.nodes[side.input] as ExpressionNode;
      const alias = n.alias ?? generateAlias(n.expression)!;
      return { txt: alias, win: winAliases.has(alias) };
    }
    if ('parameter' in side) return { txt: side.parameter, win: false };
    const ex = translateExpression(side, true);
    return { txt: ex.column, win: ex.isWindow === true };
  };

  const L = sideInfo(cond.left);
  const R = sideInfo(cond.right);
  const sql = `${L.txt} ${mapFilterOperatorToSQL(cond.op)} ${R.txt}`;

  return { sql, touchesWindow: L.win || R.win };
}

function mapFilterOperatorToSQL(operator: string): string {
  switch (operator) {
    case 'eq':
      return '=';
    case 'neq':
      return '!=';
    case 'gt':
      return '>';
    case 'gte':
      return '>=';
    case 'lt':
      return '<';
    case 'lte':
      return '<=';
    case 'in':
      return 'IN';
    case 'nin':
      return 'NOT IN';
    case 'contains':
      return 'LIKE'; // Adjust if Clickhouse has a different contains operator
    case 'ncontains':
      return 'NOT LIKE'; // Adjust accordingly
    default:
      return operator; // Fallback to the original string if no mapping
  }
}

function mapTimeUnitToSeconds(unit: TimeUnit, trading = false): number {
  switch (unit) {
    case 'second':
      return 1;
    case 'minute':
      return 60;
    case 'hour':
      return 3600 * (trading ? getTradingUnitMultiplier(unit) : 1);
    case 'day':
      return 86400 * (trading ? getTradingUnitMultiplier(unit) : 1);
    case 'week':
      return 604800 * (trading ? getTradingUnitMultiplier(unit) : 1);
    case 'month':
      return 2592000;
    case 'year':
      return 31536000;
    default:
      return 0;
  }
}

function getTradingUnitMultiplier(unit: TimeUnit) {
  switch (unit) {
    case 'hour':
      return 3;
    case 'day':
      return 1.5;
    case 'week':
      return 1.3;
    default:
      return 1;
  }
}

function chooseLargestTimeRange(timeRanges: TimeRange[]): TimeRange | undefined {
  let largestRange: TimeRange | undefined = undefined;
  let largestDuration = 0;

  for (const range of timeRanges) {
    if (range.type === 'relative') {
      const duration = range.duration * mapTimeUnitToSeconds(range.unit);
      if (duration > largestDuration) {
        largestRange = range;
        largestDuration = duration;
      }
    } else if (range.type === 'trading') {
      // Trading days/hours are different from regular days/hours
      // To simplify, we'll just multiply by 3 to get an approximate duration in seconds
      const duration = range.duration * mapTimeUnitToSeconds(range.unit, true);
      if (duration > largestDuration) {
        largestRange = range;
        largestDuration = duration;
      }
    } else if (range.type === 'absolute') {
      const duration = range.to - range.from;
      if (duration > largestDuration) {
        largestRange = range;
        largestDuration = duration;
      }
    }
  }

  return largestRange;
}

export function timeRangeToSQL(timeRange: TimeRange, leeway = 1, timeColumn = 'date'): string {
  if (timeRange.type === 'relative') {
    // Add extra day as lee-way
    return `${timeColumn} >= toDate(date_sub(now(), INTERVAL ${timeRange.duration + leeway} ${timeRange.unit.toUpperCase()}))`;
  } else if (timeRange.type === 'absolute') {
    let from = new Date(timeRange.from * 1000 - 86400).toISOString().slice(0, 10);
    let to = new Date(timeRange.to * 1000).toISOString().slice(0, 10);
    return `${timeColumn} BETWEEN toDate('${from}') AND toDate('${to}')`;
  } else {
    let duration = Math.round(
      (timeRange.duration + leeway) * getTradingUnitMultiplier(timeRange.unit)
    );
    return `${timeColumn} >= toDate(date_sub(now(), INTERVAL ${duration} ${timeRange.unit.toUpperCase()}))`;
  }
}
