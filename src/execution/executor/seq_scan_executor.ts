import { TableHeap } from "../../storage/table/table_heap";
import { SeqScanPlanNode } from "../plan/sec_scan_plan_node";

export class SecScanExecutor {
  constructor(
    private _secScanPlanNode: SeqScanPlanNode,
    private _tableHeap: TableHeap
  ) {}
}
