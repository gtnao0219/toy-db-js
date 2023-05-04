import { DeleteExecutor } from "./executor/delete_executor";
import { Executor } from "./executor/executor";
import { FilterExecutor } from "./executor/filter_executor";
import { InsertExecutor } from "./executor/insert_executor";
import { LimitExecutor } from "./executor/limit_executor";
import { ProjectionExecutor } from "./executor/projection_executor";
import { SeqScanExecutor } from "./executor/seq_scan_executor";
import { SortExecutor } from "./executor/sort_executor";
import { UpdateExecutor } from "./executor/update_executor";
import { ExecutorContext } from "./executor_context";
import { PlanNode } from "./plan";

export function createExecutor(
  executorContext: ExecutorContext,
  plan: PlanNode
): Executor {
  switch (plan.type) {
    case "insert":
      return new InsertExecutor(executorContext, plan);
    case "delete":
      return new DeleteExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    case "update":
      return new UpdateExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    case "seq_scan":
      return new SeqScanExecutor(executorContext, plan);
    case "project":
      return new ProjectionExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    case "filter":
      return new FilterExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    case "sort":
      return new SortExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    case "limit":
      return new LimitExecutor(
        executorContext,
        plan,
        createExecutor(executorContext, plan.child)
      );
    default:
      throw new Error("Unsupported plan type");
  }
}
