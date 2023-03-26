import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { DeleteExecutor } from "./executor/delete_executor";
import { Executor } from "./executor/executor";
import { FilterExecutor } from "./executor/filter_executor";
import { InsertExecutor } from "./executor/insert_executor";
import { ProjectionExecutor } from "./executor/projection_executor";
import { SeqScanExecutor } from "./executor/seq_scan_executor";
import { UpdateExecutor } from "./executor/update_executor";
import { DeletePlanNode } from "./plan/delete_plan_node";
import { FilterPlanNode } from "./plan/filter_plan";
import { InsertPlanNode } from "./plan/insert_plan_node";
import { PlanNode, PlanType } from "./plan/plan_node";
import { ProjectionPlanNode } from "./plan/projection_plan";
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
        plan as DeletePlanNode,
        createExecutor(
          catalog,
          bufferPoolManager,
          (plan as DeletePlanNode).child
        )
      );
    case PlanType.UPDATE:
      return new UpdateExecutor(
        catalog,
        bufferPoolManager,
        plan as UpdatePlanNode,
        createExecutor(
          catalog,
          bufferPoolManager,
          (plan as UpdatePlanNode).child
        )
      );
    case PlanType.SEQ_SCAN:
      return new SeqScanExecutor(
        catalog,
        bufferPoolManager,
        plan as SeqScanPlanNode
      );
    case PlanType.PROJECTION:
      return new ProjectionExecutor(
        catalog,
        bufferPoolManager,
        plan as ProjectionPlanNode,
        createExecutor(
          catalog,
          bufferPoolManager,
          (plan as ProjectionPlanNode).child
        )
      );
    case PlanType.FILTER:
      return new FilterExecutor(
        catalog,
        bufferPoolManager,
        plan as FilterPlanNode,
        createExecutor(
          catalog,
          bufferPoolManager,
          (plan as FilterPlanNode).child
        )
      );
    default:
      throw new Error("Unsupported plan type");
  }
}
