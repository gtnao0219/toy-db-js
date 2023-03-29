import { DeleteExecutor } from "./executor/delete_executor";
import { Executor } from "./executor/executor";
import { FilterExecutor } from "./executor/filter_executor";
import { InsertExecutor } from "./executor/insert_executor";
import { ProjectionExecutor } from "./executor/projection_executor";
import { SeqScanExecutor } from "./executor/seq_scan_executor";
import { UpdateExecutor } from "./executor/update_executor";
import { ExecutorContext } from "./executor_context";
import { DeletePlanNode } from "./plan/delete_plan_node";
import { FilterPlanNode } from "./plan/filter_plan";
import { InsertPlanNode } from "./plan/insert_plan_node";
import { PlanNode, PlanType } from "./plan/plan_node";
import { ProjectionPlanNode } from "./plan/projection_plan";
import { SeqScanPlanNode } from "./plan/seq_scan_plan_node";
import { UpdatePlanNode } from "./plan/update_plan_node";

export function createExecutor(
  executorContext: ExecutorContext,
  plan: PlanNode
): Executor {
  switch (plan.planType) {
    case PlanType.INSERT:
      return new InsertExecutor(executorContext, plan as InsertPlanNode);
    case PlanType.DELETE:
      return new DeleteExecutor(
        executorContext,
        plan as DeletePlanNode,
        createExecutor(executorContext, (plan as DeletePlanNode).child)
      );
    case PlanType.UPDATE:
      return new UpdateExecutor(
        executorContext,
        plan as UpdatePlanNode,
        createExecutor(executorContext, (plan as UpdatePlanNode).child)
      );
    case PlanType.SEQ_SCAN:
      return new SeqScanExecutor(executorContext, plan as SeqScanPlanNode);
    case PlanType.PROJECTION:
      return new ProjectionExecutor(
        executorContext,
        plan as ProjectionPlanNode,
        createExecutor(executorContext, (plan as ProjectionPlanNode).child)
      );
    case PlanType.FILTER:
      return new FilterExecutor(
        executorContext,
        plan as FilterPlanNode,
        createExecutor(executorContext, (plan as FilterPlanNode).child)
      );
    default:
      throw new Error("Unsupported plan type");
  }
}
