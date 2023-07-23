import { Catalog } from "../catalog/catalog";
import { PlanNode, cloneFromChildren, getChildren } from "../execution/plan";

export async function optimizeFilterIndexScan(
  plan: PlanNode,
  catalog: Catalog
): Promise<PlanNode> {
  const children: PlanNode[] = [];
  for (const child of getChildren(plan)) {
    children.push(await optimizeFilterIndexScan(child, catalog));
  }
  const newPlan = cloneFromChildren(plan, children);
  if (
    newPlan.type === "filter" &&
    newPlan.child.type === "seq_scan" &&
    newPlan.condition.type === "binary_operation" &&
    newPlan.condition.operator === "=" &&
    newPlan.condition.left.type === "path" &&
    newPlan.condition.right.type === "literal"
  ) {
    const schema = await catalog.getSchemaByOid(newPlan.child.tableOid);
    const indexes = await catalog.getIndexesByOid(newPlan.child.tableOid);
    for (const index of indexes) {
      if (
        index.columnName ===
        schema.columns[newPlan.condition.left.columnIndex].name
      ) {
        return {
          type: "index_scan",
          indexOid: index.oid,
          condition: newPlan.condition,
          tableOid: newPlan.child.tableOid,
          outputSchema: newPlan.child.outputSchema,
        };
      }
    }
  }
  return newPlan;
}
