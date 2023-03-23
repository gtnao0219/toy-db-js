import { createInterface } from "readline";
import { TablePage } from "./storage/page/table_page";
import { DiskManager } from "./storage/disk/disk_manager";
import { BufferPoolManager } from "./buffer/buffer_pool_manager";
import { Tuple } from "./storage/table/tuple";
import { Schema } from "./catalog/schema";
import { Column } from "./catalog/column";
import { Type } from "./type/type";
import { BooleanValue, IntegerValue } from "./type/value";
import { TableHeap } from "./storage/table/table_heap";

// TEMP
const schema = new Schema([
  new Column("id", Type.INTEGER),
  new Column("deleted", Type.BOOLEAN),
]);

console.log("start toy-db.");
const diskManager = new DiskManager();
if (diskManager.init()) {
  console.log("data file created.");
  const tablePage = TablePage.newEmptyTablePage(0);
  diskManager.writePage(tablePage);
}
const bufferPoolManager = new BufferPoolManager(diskManager);

const rl = createInterface(process.stdin);
process.stdout.write("> ");
rl.on("line", (line) => {
  const tokens = line.trim().split(" ");
  if (tokens.length === 1 && tokens[0] === "select") {
    const tableHeap = TableHeap.get(bufferPoolManager, schema, 0);
    const tuples = tableHeap.scan();
    console.log(
      tuples
        .map((tuple) => tuple.values.map((value) => value.value).join(", "))
        .join("\n")
    );
  } else if (tokens.length === 3 && tokens[0] === "insert") {
    const tableHeap = TableHeap.get(bufferPoolManager, schema, 0);
    tableHeap.insertTuple(
      new Tuple(null, schema, [
        new IntegerValue(parseInt(tokens[1])),
        new BooleanValue(tokens[2] === "true"),
      ])
    );
    console.log("inserted!");
  } else if (tokens.length === 1 && tokens[0] === "debug") {
    console.log(bufferPoolManager);
  } else if (tokens.length === 1 && tokens[0] === "exit") {
    bufferPoolManager.flushAllPages();
    console.log("bye!");
    process.exit(0);
  } else {
    console.log("invalid query.");
  }
  process.stdout.write("> ");
});
