import type { ComputeGraph } from './compute-graph';

/**
 * Render a ComputeGraph as a Mermaid flow diagram using consistent shapes and labels.
 */
export function renderMermaid(graph: ComputeGraph): string {
  const nodes = graph.getExecutionOrder();
  const lines = ['graph TD;'];

  const addedNodes = new Set<string>();
  const addedEdges = new Set<string>();
  const renderIdMap: Record<NodeID, string> = {};
  const sourceNameCounts: Record<string, number> = {};

  const getRenderId = (node: ComputeNode): string => {
    if (renderIdMap[node.id]) return renderIdMap[node.id];
    if (node.type === 'source') {
      const base = (node as any).table.replace(/[^a-zA-Z0-9_]/g, '_');
      const count = (sourceNameCounts[base] || 0) + 1;
      sourceNameCounts[base] = count;
      const id = count === 1 ? base : `${base}_${count}`;
      renderIdMap[node.id] = id;
      return id;
    }
    renderIdMap[node.id] = node.id;
    return node.id;
  };

  const addNode = (node: ComputeNode) => {
    const renderId = getRenderId(node);
    if (addedNodes.has(renderId)) return;

    const label = getNodeLabel(node, graph);
    const escapedLabel = label.replace(/[<>]/g, '');

    let shape: string;
    switch (node.type) {
      case 'source':
        shape = '[(%s)]';
        break;
      case 'filter':
        shape = '[\\"%s"/]';
        break;
      case 'composite-filter':
        shape = '((%s))';
        break;
      case 'projection':
        shape = '[["%s"]]';
        break;
      case 'expression':
        shape = '("%s")';
        break;
      case 'sort':
        shape = '["%s"]';
        break;
      case 'limit':
        shape = '([%s])';
        break;
      default:
        shape = '["%s"]';
    }

    lines.push(`    ${renderId}${shape.replace('%s', escapedLabel)}`);
    addedNodes.add(renderId);
  };

  const addEdge = (from: NodeID, to: NodeID) => {
    const fromRender = renderIdMap[from];
    const toRender = renderIdMap[to];
    const edgeKey = `${fromRender}->${toRender}`;
    if (addedEdges.has(edgeKey)) return;
    lines.push(`    ${fromRender} --> ${toRender}`);
    addedEdges.add(edgeKey);
  };

  nodes.forEach((node) => addNode(node));
  nodes.forEach((node) => {
    node.inputs.forEach((inputId) => addEdge(inputId, node.id));
  });

  const edgeLines = lines.slice(1).sort();
  return [lines[0], ...edgeLines].join('\n');
}

/**
 * Human-friendly label per node type for the diagram.
 */
function getNodeLabel(node: ComputeNode, graph: ComputeGraph): string {
  switch (node.type) {
    case 'source':
      return `${(node as any).table}`;
    case 'filter':
      return formatCondition((node as FilterNode).condition, graph);
    case 'composite-filter': {
      const compositeNode = node as CompositeFilterNode;
      if (compositeNode.inputs.length === 0) return 'Empty';
      return compositeNode.operator.toUpperCase();
    }
    case 'projection':
      return formatProjectionLabel(node as ProjectionNode);
    case 'expression': {
      const exprNode = node as any;
      return `${graph.getExpressionLabel(exprNode.expression)}${
        exprNode.alias ? ` as ${exprNode.alias}` : ''
      }`;
    }
    case 'sort':
      return formatSortLabel(node as SortNode);
    case 'limit':
      return formatLimitLabel(node as LimitNode);
    case 'join': {
      const joinNode = node as JoinNode;
      const leftTable = getNodeDisplayName(graph, joinNode.joinConditions?.[0].left.input!);
      const rightTable = getNodeDisplayName(graph, joinNode.joinConditions?.[0].right.input!);
      const cond = joinNode.joinConditions
        ?.map((c) => `${c.left.column} ${c.op} ${c.right.column}`)
        .join(' and ');
      return `Join ${leftTable} with ${rightTable} on ${cond}`;
    }
    default:
      return (node as any).type;
  }
}

/**
 * Prefer table name for sources when available.
 */
function getNodeDisplayName(graph: ComputeGraph, nodeId: NodeID): string {
  const node = graph.getNodes()[nodeId];
  if (!node) return nodeId;
  if (node.type === 'source') return (node as any).table;
  return node.id;
}

/**
 * Label grouping projections distinctly to surface GROUP BY dimensions.
 */
function formatProjectionLabel(node: ProjectionNode): string {
  const cols = node.columns
    .map((c) => ('name' in c ? c.name : c.alias || 'expr') + (c.alias ? ` as ${c.alias}` : ''))
    .join(', ');
  if (node.metadata?.isGrouping) {
    return `GROUP BY\n${cols}`;
  }
  return `Project\n${cols}`;
}

function formatSortLabel(node: SortNode): string {
  const base = `Sort by ${node.criteria.map((c) => `${c.expression} ${c.direction}`).join(', ')}`;
  if (node.metadata?.isGrouped && node.metadata.groupDimension) {
    return `${base}\n(within ${node.metadata.groupDimension})`;
  }
  return base;
}

function formatLimitLabel(node: LimitNode): string {
  const base = `Limit ${node.limit}`;
  if (node.metadata?.isGrouped && node.metadata.groupDimension) {
    return `${base} BY ${node.metadata.groupDimension}`;
  }
  return base;
}

/**
 * Format a filter condition for node labels, resolving metrics/parameters.
 */
function formatCondition(condition: FilterNode['condition'], graph: ComputeGraph): string {
  let left;
  let right;
  if ('input' in condition.left) {
    left = condition.left.metric;
  } else if ('parameter' in condition.left) {
    left = condition.left.parameter;
  } else {
    left = graph.getExpressionLabel(condition.left);
  }
  if ('input' in condition.right) {
    right = getNodeLabel(graph.getNodes()[condition.right.input], graph);
  } else if ('parameter' in condition.right) {
    right = condition.right.parameter;
  } else {
    right = `"${graph.getExpressionLabel(condition.right)}"`;
  }
  return `${left} ${condition.op} ${right}`;
}
