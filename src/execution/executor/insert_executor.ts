// import { Catalog } from "../../catalog/catalog";
// import { IntegerValue } from "../../type/integer_value";
// import { string2Type } from "../../type/type";
// import { InsertPlanNode } from "../plan/insert_plan_node";
// import { Executor } from "./executor";

// export class InsertExecutor extends Executor {
//   constructor(protected _catalog: Catalog, private _plan: InsertPlanNode) {
//     super(_catalog);
//   }
//   next(): void {
//     const table = this._catalog.getTableMetadata(this._plan.tableOid);
//     if (table == null) {
//       return;
//     }
//     table.schema.columns.map((column, index) => {
//       const value = this._plan.rawValues[index];
//       if (column.type === string2Type("INTEGER")) {
//         return IntegerValue.of(value);
//       } else if (column.type === string2Type("STRING")) {
//         return new StringValue(value);
//       } else if (column.type === string2Type("BOOLEAN")) {
//         return new BooleanValue(value === "true");
//       } else {
//         throw new Error("unknown type.");
//       }
//     });
//   }
//   execute(): void {
//     const values =
//       metadata?.schema.columns.map((column, index) => {
//         const value = rawValues[index];
//         if (column.type === string2Type("INTEGER")) {
//           return new IntegerValue(parseInt(value));
//         } else if (column.type === string2Type("STRING")) {
//           return new StringValue(value);
//         } else if (column.type === string2Type("BOOLEAN")) {
//           return new BooleanValue(value === "true");
//         } else {
//           throw new Error("unknown type.");
//         }
//       }) || [];

//     metadata?.tableHeap.insertTuple(new Tuple(null, metadata.schema, values));
//   }
// }
