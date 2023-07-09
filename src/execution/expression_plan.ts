import {
  BoundBinaryOperationExpression,
  BoundExpression,
  BoundFunctionCallExpression,
  BoundLiteralExpression,
  BoundPathExpression,
  BoundUnaryOperationExpression,
} from "../binder/bound";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import { BinaryOperator, UnaryOperator } from "../parser/ast";
import { LiteralValue } from "../parser/token";
import { Tuple } from "../storage/table/tuple";
import { BooleanValue } from "../type/boolean_value";
import { IntegerValue } from "../type/integer_value";
import { Type } from "../type/type";
import { Value } from "../type/value";
import { VarcharValue } from "../type/varchar_value";
import { AggregationType, PlanNode } from "./plan";

export type ExpressionPlanNode =
  | BinaryOperationExpressionPlanNode
  | UnaryOperationExpressionPlanNode
  | FunctionCallExpressionPlanNode
  | LiteralExpressionPlanNode
  | PathExpressionPlanNode;
export type BinaryOperationExpressionPlanNode = {
  type: "binary_operation";
  operator: BinaryOperator;
  left: ExpressionPlanNode;
  right: ExpressionPlanNode;
};
export type UnaryOperationExpressionPlanNode = {
  type: "unary_operation";
  operator: UnaryOperator;
  operand: ExpressionPlanNode;
};
export type FunctionCallExpressionPlanNode = {
  type: "function_call";
  functionName: string;
  args: ExpressionPlanNode[];
};
export type LiteralExpressionPlanNode = {
  type: "literal";
  value: LiteralValue;
};
export type PathExpressionPlanNode = {
  type: "path";
  tupleIndex: number;
  columnIndex: number;
  dataType: Type;
};

export const UNNAMED = "unnamed";

export function planExpression(
  expression: BoundExpression,
  children: PlanNode[],
  aggContext?: { count: number; exprInAgg: PathExpressionPlanNode[] }
): [string, ExpressionPlanNode] {
  switch (expression.type) {
    case "binary_operation":
      return planBinaryOperationExpression(expression, children, aggContext);
    case "unary_operation":
      return planUnaryOperationExpression(expression, children, aggContext);
    case "function_call":
      return planFunctionCallExpression(expression, children, aggContext);
    case "literal":
      return planLiteralExpression(expression, children);
    case "path":
      return planPathExpression(expression, children);
  }
}

export function planBinaryOperationExpression(
  expression: BoundBinaryOperationExpression,
  children: PlanNode[],
  aggContext?: { count: number; exprInAgg: PathExpressionPlanNode[] }
): [string, BinaryOperationExpressionPlanNode] {
  const [_, left] = planExpression(expression.left, children, aggContext);
  const [__, right] = planExpression(expression.right, children, aggContext);
  return [
    UNNAMED,
    {
      type: "binary_operation",
      operator: expression.operator,
      left,
      right,
    },
  ];
}
export function planUnaryOperationExpression(
  expression: BoundUnaryOperationExpression,
  children: PlanNode[],
  aggContext?: { count: number; exprInAgg: PathExpressionPlanNode[] }
): [string, UnaryOperationExpressionPlanNode] {
  const [_, operand] = planExpression(expression.operand, children, aggContext);
  return [
    UNNAMED,
    {
      type: "unary_operation",
      operator: expression.operator,
      operand,
    },
  ];
}
export function planFunctionCallExpression(
  expression: BoundFunctionCallExpression,
  children: PlanNode[],
  aggContext?: { count: number; exprInAgg: PathExpressionPlanNode[] }
): [string, PathExpressionPlanNode] {
  if (aggContext == null) {
    throw new Error("aggContext is null");
  }
  return [UNNAMED, aggContext.exprInAgg[aggContext.count++]];
}
export function planLiteralExpression(
  expression: BoundLiteralExpression,
  children: PlanNode[]
): [string, LiteralExpressionPlanNode] {
  return [
    UNNAMED,
    {
      type: "literal",
      value: expression.value,
    },
  ];
}
export function planPathExpression(
  expression: BoundPathExpression,
  children: PlanNode[]
): [string, PathExpressionPlanNode] {
  if (children.length === 1) {
    const index = children[0].outputSchema.columns.findIndex(
      (column) => column.name === expression.path.join(".")
    );
    if (index === -1) {
      throw new Error("Not found");
    }
    return [
      expression.path.join("."),
      {
        type: "path",
        tupleIndex: 0,
        columnIndex: index,
        dataType: children[0].outputSchema.columns[index].type,
      },
    ];
  }
  if (children.length === 2) {
    const leftIndex = children[0].outputSchema.columns.findIndex(
      (column) => column.name === expression.path.join(".")
    );
    const rightIndex = children[1].outputSchema.columns.findIndex(
      (column) => column.name === expression.path.join(".")
    );
    if (leftIndex !== -1 && rightIndex !== -1) {
      throw new Error("Ambiguous");
    }
    if (leftIndex !== -1) {
      return [
        expression.path.join("."),
        {
          type: "path",
          tupleIndex: 0,
          columnIndex: leftIndex,
          dataType: children[0].outputSchema.columns[leftIndex].type,
        },
      ];
    }
    if (rightIndex !== -1) {
      return [
        expression.path.join("."),
        {
          type: "path",
          tupleIndex: 1,
          columnIndex: rightIndex,
          dataType: children[1].outputSchema.columns[rightIndex].type,
        },
      ];
    }
  }
  throw new Error("Not implemented");
}

export function evaluate(
  expression: ExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  switch (expression.type) {
    case "binary_operation":
      return evaluateBinaryOperationExpression(expression, tuple, schema);
    case "unary_operation":
      return evaluateUnaryOperationExpression(expression, tuple, schema);
    case "function_call":
      return evaluateFunctionCallExpression(expression, tuple, schema);
    case "literal":
      return evaluateLiteralExpression(expression, tuple, schema);
    case "path":
      return evaluatePathExpression(expression, tuple, schema);
  }
}
function evaluateBinaryOperationExpression(
  expression: BinaryOperationExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  const left = evaluate(expression.left, tuple, schema);
  const right = evaluate(expression.right, tuple, schema);
  switch (expression.operator) {
    case "+":
      return left.add(right);
    case "-":
      return left.subtract(right);
    case "*":
      return left.multiply(right);
    case "=":
      return left.equal(right);
    case "<>":
      return left.notEqual(right);
    case "<":
      return left.lessThan(right);
    case ">":
      return left.greaterThan(right);
    case "<=":
      return left.lessThanEqual(right);
    case ">=":
      return left.greaterThanEqual(right);
    case "AND":
      return left.and(right);
    case "OR":
      return left.or(right);
  }
}
function evaluateUnaryOperationExpression(
  expression: UnaryOperationExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  const operand = evaluate(expression.operand, tuple, schema);
  switch (expression.operator) {
    case "NOT":
      return operand.not();
  }
}
function evaluateFunctionCallExpression(
  expression: FunctionCallExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  throw new Error("Not implemented");
}
function evaluateLiteralExpression(
  expression: LiteralExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  if (typeof expression.value === "boolean") {
    return new BooleanValue(expression.value);
  }
  if (typeof expression.value === "number") {
    return new IntegerValue(expression.value);
  }
  if (typeof expression.value === "string") {
    return new VarcharValue(expression.value);
  }
  if (expression.value === null) {
    throw new Error("Not implemented");
  }
  throw new Error("Not implemented");
}
function evaluatePathExpression(
  expression: PathExpressionPlanNode,
  tuple: Tuple,
  schema: Schema
): Value {
  return tuple.values[expression.columnIndex];
}
export function evaluateJoin(
  expression: ExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  switch (expression.type) {
    case "binary_operation":
      return evaluateJoinBinaryOperationExpression(
        expression,
        leftTuple,
        leftSchema,
        rightTuple,
        rightSchema
      );
    case "unary_operation":
      return evaluateJoinUnaryOperationExpression(
        expression,
        leftTuple,
        leftSchema,
        rightTuple,
        rightSchema
      );
    case "function_call":
      return evaluateJoinFunctionCallExpression(
        expression,
        leftTuple,
        leftSchema,
        rightTuple,
        rightSchema
      );
    case "literal":
      return evaluateJoinLiteralExpression(
        expression,
        leftTuple,
        leftSchema,
        rightTuple,
        rightSchema
      );
    case "path":
      return evaluateJoinPathExpression(
        expression,
        leftTuple,
        leftSchema,
        rightTuple,
        rightSchema
      );
  }
}
function evaluateJoinBinaryOperationExpression(
  expression: BinaryOperationExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  const left = evaluateJoin(
    expression.left,
    leftTuple,
    leftSchema,
    rightTuple,
    rightSchema
  );
  const right = evaluateJoin(
    expression.right,
    leftTuple,
    leftSchema,
    rightTuple,
    rightSchema
  );
  switch (expression.operator) {
    case "+":
      return left.add(right);
    case "-":
      return left.subtract(right);
    case "*":
      return left.multiply(right);
    case "=":
      return left.equal(right);
    case "<>":
      return left.notEqual(right);
    case "<":
      return left.lessThan(right);
    case ">":
      return left.greaterThan(right);
    case "<=":
      return left.lessThanEqual(right);
    case ">=":
      return left.greaterThanEqual(right);
    case "AND":
      return left.and(right);
    case "OR":
      return left.or(right);
  }
}
function evaluateJoinUnaryOperationExpression(
  expression: UnaryOperationExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  const operand = evaluateJoin(
    expression.operand,
    leftTuple,
    leftSchema,
    rightTuple,
    rightSchema
  );
  switch (expression.operator) {
    case "NOT":
      return operand.not();
  }
}
function evaluateJoinFunctionCallExpression(
  expression: FunctionCallExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  throw new Error("Not implemented");
}
function evaluateJoinLiteralExpression(
  expression: LiteralExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  if (typeof expression.value === "boolean") {
    return new BooleanValue(expression.value);
  }
  if (typeof expression.value === "number") {
    return new IntegerValue(expression.value);
  }
  if (typeof expression.value === "string") {
    return new VarcharValue(expression.value);
  }
  if (expression.value === null) {
    throw new Error("Not implemented");
  }
  throw new Error("Not implemented");
}
function evaluateJoinPathExpression(
  expression: PathExpressionPlanNode,
  leftTuple: Tuple,
  leftSchema: Schema,
  rightTuple: Tuple,
  rightSchema: Schema
): Value {
  return expression.tupleIndex === 0
    ? leftTuple.values[expression.columnIndex]
    : rightTuple.values[expression.columnIndex];
}
export function inferType(expression: ExpressionPlanNode): Type {
  switch (expression.type) {
    case "binary_operation":
      return inferBinaryOperationType(expression);
    case "unary_operation":
      return inferUnaryOperationType(expression);
    case "function_call":
      return Type.INTEGER;
    case "literal":
      return inferLiteralType(expression);
    case "path":
      return expression.dataType;
  }
}
function inferBinaryOperationType(
  expression: BinaryOperationExpressionPlanNode
): Type {
  switch (expression.operator) {
    case "+":
    case "-":
    case "*":
      return Type.INTEGER;
    case "=":
    case "<>":
    case "<":
    case ">":
    case "<=":
    case ">=":
    case "AND":
    case "OR":
      return Type.BOOLEAN;
  }
}
function inferUnaryOperationType(
  expression: UnaryOperationExpressionPlanNode
): Type {
  switch (expression.operator) {
    case "NOT":
      return Type.BOOLEAN;
  }
}
function inferLiteralType(expression: LiteralExpressionPlanNode): Type {
  if (typeof expression.value === "boolean") {
    return Type.BOOLEAN;
  }
  if (typeof expression.value === "number") {
    return Type.INTEGER;
  }
  if (typeof expression.value === "string") {
    return Type.VARCHAR;
  }
  if (expression.value === null) {
    // TODO:
    return Type.BOOLEAN;
  }
  throw new Error("Not implemented4");
}
export function inferAggSchema(
  groupByExpressions: PathExpressionPlanNode[],
  inputExpressions: ExpressionPlanNode[],
  aggregationTypes: AggregationType[]
): Schema {
  const columns: Column[] = [];
  for (let i = 0; i < groupByExpressions.length; i++) {
    columns.push(
      new Column(`__group_by_${i}`, inferType(groupByExpressions[i]))
    );
  }
  for (let i = 0; i < inputExpressions.length; i++) {
    columns.push(new Column(`__agg_${i}`, Type.INTEGER));
  }
  return new Schema(columns);
}
