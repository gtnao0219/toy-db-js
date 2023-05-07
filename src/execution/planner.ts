import {
  BoundDeleteStatement,
  BoundInsertStatement,
  BoundSelectStatement,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
} from "../binder/bound";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import { Type } from "../type/type";
import {
  UNNAMED,
  inferType,
  planExpression,
  planPathExpression,
} from "./expression_plan";
import {
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
  child = planProject(statement, child);
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
