import { BoundColumnRefExpression } from "../../binder/bound_expression";
import { BoundBaseTableRef } from "../../binder/bound_table_ref";
import { DeleteStatement } from "../../binder/statement/delete_statement";
import { InsertStatement } from "../../binder/statement/insert_statement";
import { SelectStatement } from "../../binder/statement/select_statement";
import { Statement, StatementType } from "../../binder/statement/statement";
import { UpdateStatement } from "../../binder/statement/update_statement";
import { Schema } from "../../catalog/schema";
import { DeletePlanNode } from "./delete_plan_node";
import { InsertPlanNode } from "./insert_plan_node";
import { PlanNode } from "./plan_node";
import { SeqScanPlanNode } from "./seq_scan_plan_node";
import { UpdatePlanNode } from "./update_plan_node";

export function getPlan(statement: Statement): PlanNode {
  switch (statement.statementType) {
    case StatementType.INSERT:
      if (!(statement instanceof InsertStatement)) {
        throw new Error("Invalid statement type");
      }
      return new InsertPlanNode(statement.tableOid, statement.values);
    case StatementType.DELETE:
      if (!(statement instanceof DeleteStatement)) {
        throw new Error("Invalid statement type");
      }
      return new DeletePlanNode(statement.tableOid);
    case StatementType.UPDATE:
      if (!(statement instanceof UpdateStatement)) {
        throw new Error("Invalid statement type");
      }
      return new UpdatePlanNode(statement.tableOid, statement.assignments);
    case StatementType.SELECT:
      if (!(statement instanceof SelectStatement)) {
        throw new Error("Invalid statement type");
      }
      const oid = (statement.table as BoundBaseTableRef).tableOid;
      const schema = new Schema(
        statement.selectList.map((expression) => {
          const columnIndex = (expression as BoundColumnRefExpression)
            .columnIndex;
          return (statement.table as BoundBaseTableRef).schema.columns[
            columnIndex
          ];
        })
      );
      return new SeqScanPlanNode(oid, schema);
    default:
      throw new Error("Unsupported statement type");
  }
}
