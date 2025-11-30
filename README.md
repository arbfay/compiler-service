# Compiler Service

This service compiles user queries that were pre-formatted into optimized ClickHouse SQL queries and visual computation graphs. It is a stripped-down version of the compiler we used for our agentic data analysis platform, focused specifically on processing market data.

## Features

- ✅ Compiles compute graphs to ClickHouse SQL queries
- ✅ Generates Mermaid diagrams of computation graphs
- ✅ Graph optimization to reduce redundant operations
- ✅ Parameter extraction for safe query execution
- ✅ Support for complex filters, aggregations, and window functions
- ✅ Type-safe with TypeScript

## Getting Started

### Docker (Recommended)

The easiest way to run the service is using Docker:

**Build the image:**
```bash
docker build -t compiler-service .
```

**Run the container:**
```bash
docker run -p 3000:3000 compiler-service
```

The service will be available at `http://localhost:3000`.

### Local Development

#### Install Dependencies

```bash
bun install
```

#### Run Development Server

```bash
bun run dev
```

The service will start on `http://localhost:3000`.

#### Run Tests

```bash
bun test
```

#### Type Check

```bash
bun typecheck
```

## API Endpoints

### `GET /`

Returns service information and available endpoints.

**Response:**
```json
{
  "message": "Screener Compiler Service",
  "endpoints": {
    "compile": "POST /compile - Compile a screener to SQL and graph",
    "health": "GET /health - Health check"
  }
}
```

### `GET /health`

Health check endpoint.

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-11-28T12:00:00.000Z"
}
```

### `POST /compile`

Compiles a screener definition into SQL and computation graph.

**Request Body:**

A screener object with the following structure:

```json
{
  "id": "screener-id",
  "name": "Screener Name",
  "created_at": 0,
  "status": "active",
  "filter": { /* filter definition */ },
  "group_by": [ /* optional grouping criteria */ ],
  "sort_by": [ /* optional sort criteria */ ],
  "limit": 100
}
```

**Response:**

```json
{
  "success": true,
  "query": {
    "id": "screener-id",
    "name": "Screener Name"
  },
  "graph": "graph TD;\n  tickers[(tickers)] --> filter_1\n  ...",
  "sql": {
    "query": "WITH cte_0 AS (...) SELECT ...",
    "parameters": { "param_1": "value" }
  }
}
```

## Example Usage

### Simple Sector Filter

```bash
curl -X POST http://localhost:3000/compile \
  -H "Content-Type: application/json" \
  -d '{
    "id": "tech-stocks",
    "name": "Technology Stocks",
    "created_at": 0,
    "status": "active",
    "filter": {
      "type": "simple",
      "target": { "type": "metric", "metric": "sector" },
      "op": "eq",
      "value": { "type": "constant", "value": "Technology" }
    },
    "limit": 100
  }'
```

### Momentum Screener

```bash
curl -X POST http://localhost:3000/compile \
  -H "Content-Type: application/json" \
  -d '{
    "id": "momentum",
    "name": "High Momentum Stocks",
    "created_at": 0,
    "status": "active",
    "filter": {
      "type": "simple",
      "target": {
        "type": "aggregate",
        "target": { "type": "metric", "metric": "close" },
        "aggregation": "diff_pct",
        "time_range": { "type": "relative", "duration": 30, "unit": "day" },
        "alias": "return_30d"
      },
      "op": "gt",
      "value": { "type": "constant", "value": 10 }
    },
    "limit": 50
  }'
```

### Using the Example File

```bash
curl -X POST http://localhost:3000/compile \
  -H "Content-Type: application/json" \
  -d @example-request.json
```

## Config

This repo ships with a fixed in-repo configuration for tables/columns. To change metrics, add new columns, or adjust joins/time columns, edit `src/settings.ts` manually and restart the service.

## Screener Definition

### Filter Types

#### Simple Filter

```typescript
{
  type: 'simple',
  target: Expression,
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'nin' | 'contains' | 'ncontains',
  value: Expression
}
```

#### Composite Filter

```typescript
{
  type: 'composite',
  operator: 'and' | 'or' | 'not',
  filters: Filter[]
}
```

### Expression Types

#### Metric Expression

```typescript
{
  type: 'metric',
  metric: string,  // e.g., 'close', 'volume', 'sector'
  alias?: string
}
```

#### Constant Expression

```typescript
{
  type: 'constant',
  value: number | string | boolean | number[] | string[]
}
```

#### Math Expression

```typescript
{
  type: 'math',
  operator: '+' | '-' | '*' | '/' | '^' | '%' | 'sqrt' | 'abs' | 'ln' | 'log10',
  operands: Expression[]
}
```

#### Aggregate Expression

```typescript
{
  type: 'aggregate',
  target: Expression,
  aggregation: 'first' | 'last' | 'min' | 'max' | 'avg' | 'sum' | 'median' | 'stddev' | 'count' | 'diff' | 'diff_pct',
  time_range?: {
    type: 'relative' | 'absolute' | 'trading',
    duration?: number,
    unit?: 'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'year',
    from?: number,  // Unix timestamp for absolute
    to?: number     // Unix timestamp for absolute
  },
  alias?: string
}
```

### Grouping

```typescript
{
  dimension: string,  // Metric to group by (e.g., 'sector')
  expression?: Expression,  // Expression to order by within each group
  limit: number  // Top N within each group
}
```

### Sorting

```typescript
{
  expression: Expression,
  direction: 'asc' | 'desc'
}
```

## Architecture

The compiler follows these steps:

1. **Parse** - Convert screener JSON to internal representation
2. **Build Graph** - Create computation graph with nodes for sources, filters, projections, expressions
3. **Infer Joins** - Automatically determine required table joins
4. **Optimize** - Remove redundant nodes, merge filters, inline parameters
5. **Generate SQL** - Translate graph to optimized ClickHouse SQL with CTEs
6. **Generate Diagram** - Create Mermaid visualization of computation graph
