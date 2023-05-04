import {
  BoundDeleteStatement,
  BoundInsertStatement,
  BoundSelectStatement,
  BoundStatement,
  BoundTableReference,
  BoundUpdateStatement,
} from "../binder/bound";
import {
  DeletePlanNode,
  InsertPlanNode,
  PlanNode,
  UpdatePlanNode,
} from "./plan";

export function plan(statement: BoundStatement): PlanNode {
  switch (statement.type) {
    case "select_statement":
      return planSelectStatement(statement);
    case "insert_statement":
      return planInsertStatement(statement);
    case "delete_statement":
      return planDeleteStatement(statement);
    case "update_statement":
      return planUpdateStatement(statement);
    default:
      throw new Error("Not implemented");
  }
}

function planInsertStatement(statement: BoundInsertStatement): InsertPlanNode {
  return {
    type: "insert",
    tableOid: statement.tableOid,
    schema: statement.schema,
    values: statement.values,
  };
}
function planDeleteStatement(statement: BoundDeleteStatement): DeletePlanNode {
  let child: PlanNode = {
    type: "seq_scan",
    tableOid: statement.tableOid,
    schema: statement.schema,
  };
  if (statement.condition != null) {
    child = {
      type: "filter",
      condition: statement.condition,
      child,
    };
  }
  return {
    type: "delete",
    tableOid: statement.tableOid,
    schema: statement.schema,
    child,
  };
}

function planUpdateStatement(statement: BoundUpdateStatement): UpdatePlanNode {
  let child: PlanNode = {
    type: "seq_scan",
    tableOid: statement.tableOid,
    schema: statement.schema,
  };
  if (statement.condition != null) {
    child = {
      type: "filter",
      condition: statement.condition,
      child,
    };
  }
  return {
    type: "update",
    tableOid: statement.tableOid,
    schema: statement.schema,
    assignments: statement.assignments,
    child,
  };
}
function planSelectStatement(statement: BoundSelectStatement): PlanNode {
  let child: PlanNode = planTableReference(statement.tableReference);
  if (statement.condition != null) {
    child = {
      type: "filter",
      condition: statement.condition,
      child,
    };
  }
  child = {
    type: "project",
    selectElements: statement.selectElements,
    child,
  };
  if (statement.orderBy != null) {
    child = {
      type: "sort",
      sortKeys: statement.orderBy.sortKeys,
      child,
    };
  }
  if (statement.limit != null) {
    child = {
      type: "limit",
      count: statement.limit.value,
      child,
    };
  }
  return child;
}

function planTableReference(tableReference: BoundTableReference): PlanNode {
  switch (tableReference.type) {
    case "simple_table_reference":
      return {
        type: "seq_scan",
        tableOid: tableReference.tableOid,
        schema: tableReference.schema,
      };
    case "join_table_reference":
      throw new Error("Not implemented");
    case "subquery_table_reference":
      return plan(tableReference.query);
  }
}
