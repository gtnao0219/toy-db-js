import {
  BoundDeleteStatement,
  BoundExpression,
  BoundFunctionCallExpression,
  BoundInsertStatement,
  BoundSelectStatement,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
  hasAggregation,
} from "../binder/bound";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import { Type } from "../type/type";
import { PathExpressionPlanNode, inferAggSchema } from "./expression_plan";
import {
  ExpressionPlanNode,
  UNNAMED,
  inferType,
  planExpression,
  planPathExpression,
} from "./expression_plan";
import {
  AggregationType,
  DeletePlanNode,
  InsertPlanNode,
  PlanNode,
  ProjectPlanNode,
  UpdatePlanNode,
} from "./plan";

export function plan(statement: BoundStatement): PlanNode {
  switch (statement.type) {
    case "select_statement":
      return planSelectStatement(statement);
    case "insert_statement":
      return planInsertStatement(statement);
    case "update_statement":
      return planUpdateStatement(statement);
    case "delete_statement":
      return planDeleteStatement(statement);
    default:
      throw new Error("Not implemented");
  }
}

function planSelectStatement(statement: BoundSelectStatement): PlanNode {
  let child: PlanNode = planTableReference(statement.tableReference);
  if (statement.condition != null) {
    const [_, condition] = planExpression(statement.condition, [child]);
    child = {
      type: "filter",
      condition,
      outputSchema: child.outputSchema,
      child,
    };
  }

  // aggregation
  const isAggregationRequired =
    statement.groupBy != null ||
    statement.having != null ||
    statement.selectElements.some((selectElement) =>
      hasAggregation(selectElement.expression)
    );

  if (isAggregationRequired) {
    child = planAggregate(statement, child);
  } else {
    child = planProject(statement, child);
  }

  if (statement.orderBy != null) {
    child = {
      type: "sort",
      sortKeys: statement.orderBy.sortKeys.map((sortKey) => {
        const [_, expression] = planPathExpression(sortKey.expression, [child]);
        return {
          expression,
          direction: sortKey.direction,
        };
      }),
      outputSchema: child.outputSchema,
      child,
    };
  }
  if (statement.limit != null) {
    const [_, count] = planExpression(statement.limit.count, [child]);
    child = {
      type: "limit",
      count,
      outputSchema: child.outputSchema,
      child,
    };
  }
  return child;
}
function planInsertStatement(statement: BoundInsertStatement): InsertPlanNode {
  return {
    type: "insert",
    tableOid: statement.tableReference.tableOid,
    outputSchema: new Schema([new Column("__count", Type.INTEGER)]),
    values: statement.values.map((value) => {
      const [_, expression] = planExpression(value, []);
      return expression;
    }),
  };
}
function planUpdateStatement(statement: BoundUpdateStatement): UpdatePlanNode {
  let child: PlanNode = planTableReference(statement.tableReference);
  if (statement.condition != null) {
    const [_, condition] = planExpression(statement.condition, [child]);
    child = {
      type: "filter",
      condition,
      outputSchema: child.outputSchema,
      child,
    };
  }
  return {
    type: "update",
    tableOid: statement.tableReference.tableOid,
    assignments: statement.assignments.map((assignment) => {
      const [_, target] = planPathExpression(assignment.target, [child]);
      const [__, value] = planExpression(assignment.value, [child]);
      return {
        target,
        value,
      };
    }),
    outputSchema: new Schema([new Column("__count", Type.INTEGER)]),
    child,
  };
}
function planDeleteStatement(statement: BoundDeleteStatement): DeletePlanNode {
  let child: PlanNode = planTableReference(statement.tableReference);
  if (statement.condition != null) {
    const [_, condition] = planExpression(statement.condition, [child]);
    child = {
      type: "filter",
      condition,
      outputSchema: child.outputSchema,
      child,
    };
  }
  return {
    type: "delete",
    tableOid: statement.tableReference.tableOid,
    outputSchema: new Schema([new Column("__count", Type.INTEGER)]),
    child,
  };
}

function planTableReference(tableReference: BoundTableReference): PlanNode {
  switch (tableReference.type) {
    case "base_table_reference":
      const tableName = tableReference.alias ?? tableReference.tableName;
      return {
        type: "seq_scan",
        tableOid: tableReference.tableOid,
        outputSchema: new Schema(
          tableReference.schema.columns.map((column) => {
            return new Column(`${tableName}.${column.name}`, column.type);
          })
        ),
      };
    case "join_table_reference":
      const left = planTableReference(tableReference.left);
      const right = planTableReference(tableReference.right);
      const [_, condition] = planExpression(tableReference.condition, [
        left,
        right,
      ]);
      return {
        type: "nested_loop_join",
        joinType: tableReference.joinType,
        condition,
        left,
        right,
        outputSchema: new Schema([
          ...left.outputSchema.columns,
          ...right.outputSchema.columns,
        ]),
      };
    case "subquery_table_reference":
      return planSelectStatement(tableReference.query);
  }
}
function planProject(
  selectStatement: BoundSelectStatement,
  child: PlanNode
): ProjectPlanNode {
  if (selectStatement.isAsterisk) {
    return {
      type: "project",
      selectElements: child.outputSchema.columns.map((column, index) => {
        return {
          expression: {
            type: "path",
            tupleIndex: 0,
            columnIndex: index,
            dataType: column.type,
          },
        };
      }),
      outputSchema: child.outputSchema,
      child,
    };
  }
  const names: string[] = [];
  const selectElementsPlanNode = selectStatement.selectElements.map(
    (selectElement) => {
      const [name, expression] = planExpression(selectElement.expression, [
        child,
      ]);
      names.push(name);
      return {
        expression: expression,
        alias: selectElement.alias,
      };
    }
  );
  let unnamedCount = 0;
  return {
    type: "project",
    selectElements: selectElementsPlanNode,
    outputSchema: new Schema(
      selectElementsPlanNode.map((selectElementPlanNode, index) => {
        return new Column(
          selectElementPlanNode.alias ??
            (names[index] === UNNAMED
              ? `__col${unnamedCount++}`
              : names[index]),
          inferType(selectElementPlanNode.expression)
        );
      })
    ),
    child,
  };
}

function planAggregate(
  selectStatement: BoundSelectStatement,
  child: PlanNode
): ProjectPlanNode {
  const outputSchemaNames: string[] = [];
  const groupByExpressions: PathExpressionPlanNode[] = [];
  for (const groupByItem of selectStatement.groupBy ?? []) {
    const [name, expression] = planPathExpression(groupByItem, [child]);
    outputSchemaNames.push(name);
    groupByExpressions.push(expression);
  }
  // TODO: support context
  const aggregationExpressions: BoundExpression[] = [];
  if (selectStatement.having != null) {
    addAggCallToContext(selectStatement.having).forEach((aggCall) => {
      aggregationExpressions.push(aggCall);
    });
  }
  selectStatement.selectElements.forEach((selectElement) => {
    addAggCallToContext(selectElement.expression).forEach((aggCall) => {
      aggregationExpressions.push(aggCall);
    });
  });
  const inputExpressions: ExpressionPlanNode[] = [];
  const aggregationTypes: AggregationType[] = [];
  const aggBeginIndex = groupByExpressions.length;
  // TODO: support context
  const exprInAgg: PathExpressionPlanNode[] = [];
  for (let i = 0; i < aggregationExpressions.length; i++) {
    const aggregationExpression = aggregationExpressions[i];
    if (aggregationExpression.type !== "function_call") {
      throw new Error("aggregation expression must be function call");
    }
    const [aggregationType, inputExpression] = planAggCall(
      aggregationExpression,
      child
    );
    if (inputExpression.length === 0) {
      inputExpressions.push({
        type: "literal",
        value: 1,
      });
    } else if (inputExpression.length === 1) {
      inputExpressions.push(inputExpression[0]);
    } else {
      throw new Error("invalid aggregation expression size");
    }
    aggregationTypes.push(aggregationType);
    outputSchemaNames.push(`__agg${i}`);
    exprInAgg.push({
      type: "path",
      tupleIndex: 0,
      columnIndex: aggBeginIndex + i,
      dataType: Type.INTEGER,
    });
  }
  const outputSchema = inferAggSchema(
    groupByExpressions,
    inputExpressions,
    aggregationTypes
  );
  let plan: PlanNode = {
    type: "aggregate",
    groupBy: groupByExpressions,
    aggregations: inputExpressions,
    aggregationTypes,
    outputSchema: new Schema(
      outputSchema.columns.map((column, index) => {
        return new Column(outputSchemaNames[index], column.type);
      })
    ),
    child,
  };
  const aggContext = {
    exprInAgg,
    count: 0,
  };
  if (selectStatement.having != null) {
    const [_, condition] = planExpression(
      selectStatement.having,
      [plan],
      aggContext
    );
    plan = {
      type: "filter",
      condition,
      outputSchema: plan.outputSchema,
      child: plan,
    };
  }
  const names: string[] = [];
  const selectElementsPlanNode = selectStatement.selectElements.map(
    (selectElement) => {
      const [name, expression] = planExpression(
        selectElement.expression,
        [plan],
        aggContext
      );
      names.push(name);
      return {
        expression: expression,
        alias: selectElement.alias,
      };
    }
  );
  let unnamedCount = 0;
  return {
    type: "project",
    selectElements: selectElementsPlanNode,
    outputSchema: new Schema(
      selectElementsPlanNode.map((selectElementPlanNode, index) => {
        return new Column(
          selectElementPlanNode.alias ??
            (names[index] === UNNAMED
              ? `__col${unnamedCount++}`
              : names[index]),
          inferType(selectElementPlanNode.expression)
        );
      })
    ),
    child: plan,
  };
}
function addAggCallToContext(expression: BoundExpression): BoundExpression[] {
  switch (expression.type) {
    case "binary_operation":
      return [
        ...addAggCallToContext(expression.left),
        ...addAggCallToContext(expression.right),
      ];
    case "unary_operation":
      return addAggCallToContext(expression.operand);
    case "function_call":
      return [expression];
    case "path":
      return [];
    case "literal":
      return [];
  }
}
function planAggCall(
  aggCall: BoundFunctionCallExpression,
  child: PlanNode
): [AggregationType, ExpressionPlanNode[]] {
  const exprs = aggCall.args.map((arg) => {
    const [_, expr] = planExpression(arg, [child]);
    return expr;
  });
  switch (aggCall.functionName.toLowerCase()) {
    case "count":
      return ["count", exprs];
    case "sum":
      return ["sum", exprs];
    case "min":
      return ["min", exprs];
    case "max":
      return ["max", exprs];
    default:
      throw new Error(`unknown function ${aggCall.functionName}`);
  }
}
