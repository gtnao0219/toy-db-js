import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { DeleteExecutor } from "./executor/delete_executor";
import { Executor } from "./executor/executor";
import { InsertExecutor } from "./executor/insert_executor";
import { SeqScanExecutor } from "./executor/seq_scan_executor";
import { UpdateExecutor } from "./executor/update_executor";
import { DeletePlanNode } from "./plan/delete_plan_node";
import { InsertPlanNode } from "./plan/insert_plan_node";
import { PlanNode, PlanType } from "./plan/plan_node";
import { SeqScanPlanNode } from "./plan/seq_scan_plan_node";
import { UpdatePlanNode } from "./plan/update_plan_node";

export function createExecutor(
  catalog: Catalog,
  bufferPoolManager: BufferPoolManager,
  plan: PlanNode
): Executor {
  switch (plan.planType) {
    case PlanType.INSERT:
      return new InsertExecutor(
        catalog,
        bufferPoolManager,
        plan as InsertPlanNode
      );
    case PlanType.DELETE:
      return new DeleteExecutor(
        catalog,
        bufferPoolManager,
        plan as DeletePlanNode
      );
    case PlanType.UPDATE:
      return new UpdateExecutor(
        catalog,
        bufferPoolManager,
        plan as UpdatePlanNode
      );
    case PlanType.SEQ_SCAN:
      return new SeqScanExecutor(
        catalog,
        bufferPoolManager,
        plan as SeqScanPlanNode
      );
  }
}
