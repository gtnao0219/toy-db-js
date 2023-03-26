import { BoundBaseTableRef, BoundTableRef } from "../../binder/bound_table_ref";
import { DeleteStatement } from "../../binder/statement/delete_statement";
import { InsertStatement } from "../../binder/statement/insert_statement";
import { SelectStatement } from "../../binder/statement/select_statement";
import { Statement, StatementType } from "../../binder/statement/statement";
import { UpdateStatement } from "../../binder/statement/update_statement";
import { DeletePlanNode } from "./delete_plan_node";
import { FilterPlanNode } from "./filter_plan";
import { InsertPlanNode } from "./insert_plan_node";
import { PlanNode } from "./plan_node";
import { ProjectionPlanNode } from "./projection_plan";
import { SeqScanPlanNode } from "./seq_scan_plan_node";
import { UpdatePlanNode } from "./update_plan_node";

export function planStatement(statement: Statement): PlanNode {
  switch (statement.statementType) {
    case StatementType.INSERT:
      return planInsertStatement(statement as InsertStatement);
    case StatementType.DELETE:
      return planDeleteStatement(statement as DeleteStatement);
    case StatementType.UPDATE:
      return planUpdateStatement(statement as UpdateStatement);
    case StatementType.SELECT:
      return planSelectStatement(statement as SelectStatement);
    default:
      throw new Error("Unsupported statement type");
  }
}

export function planInsertStatement(
  statement: InsertStatement
): InsertPlanNode {
  return new InsertPlanNode(statement.table, statement.values);
}
export function planDeleteStatement(
  statement: DeleteStatement
): DeletePlanNode {
  const tableRefPlan = planTableRef(statement.table);
  const filterPlan = new FilterPlanNode(statement.predicate, tableRefPlan);
  return new DeletePlanNode(statement.table, filterPlan);
}
export function planUpdateStatement(
  statement: UpdateStatement
): UpdatePlanNode {
  const tableRefPlan = planTableRef(statement.table);
  const filterPlan = new FilterPlanNode(statement.predicate, tableRefPlan);
  return new UpdatePlanNode(statement.table, statement.assignments, filterPlan);
}
export function planSelectStatement(
  statement: SelectStatement
): ProjectionPlanNode {
  const tableRefPlan = planTableRef(statement.table);
  const filterPlan = new FilterPlanNode(statement.predicate, tableRefPlan);
  const projectionPlan = new ProjectionPlanNode(
    statement.selectList,
    filterPlan
  );
  return projectionPlan;
}
export function planTableRef(tableRef: BoundTableRef): PlanNode {
  if (!(tableRef instanceof BoundBaseTableRef)) {
    throw new Error("Unsupported table ref type");
  }
  return new SeqScanPlanNode(tableRef);
}
