import {
  BoundDeleteStatement,
  BoundInsertStatement,
  BoundSelectElement,
  BoundSelectStatement,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
} from "../binder/bound";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import { Type } from "../type/type";
import { UNNAMED, inferType, planExpression } from "./expression_plan";
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
  child = planProject(statement.selectElements, child);
  if (statement.orderBy != null) {
    child = {
      type: "sort",
      sortKeys: statement.orderBy.sortKeys.map((sortKey) => {
        const [_, expression] = planExpression(sortKey.expression, [child]);
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
    child = {
      type: "limit",
      count: statement.limit.count,
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
      const [_, value] = planExpression(assignment.value, [child]);
      return {
        columnIndex: assignment.columnIndex,
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
      throw new Error("Not implemented");
    case "subquery_table_reference":
      throw new Error("Not implemented");
  }
}
function planProject(
  selectElements: BoundSelectElement[],
  child: PlanNode
): ProjectPlanNode {
  const names: string[] = [];
  const selectElementsPlanNode = selectElements.map((selectElement) => {
    const [name, expression] = planExpression(selectElement.expression, [
      child,
    ]);
    names.push(name);
    return {
      expression: expression,
      alias: selectElement.alias,
    };
  });
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
