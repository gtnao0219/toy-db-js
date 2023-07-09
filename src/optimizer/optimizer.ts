import { Catalog } from "../catalog/catalog";
import { PlanNode } from "../execution/plan";
import { optimizeFilterIndexScan } from "./merge_filter_index_scan";

export async function optimize(
  plan: PlanNode,
  catalog: Catalog
): Promise<PlanNode> {
  let optimizedPlan = plan;
  optimizedPlan = await optimizeFilterIndexScan(optimizedPlan, catalog);
  return optimizedPlan;
}
