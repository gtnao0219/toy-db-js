import { createInterface } from "readline";
import { DiskManager } from "./storage/disk/disk_manager";
import { BufferPoolManager } from "./buffer/buffer_pool_manager";
import { Tuple } from "./storage/table/tuple";
import { Schema } from "./catalog/schema";
import { Column } from "./catalog/column";
import { string2Type } from "./type/type";
import { Catalog } from "./catalog/catalog";
import { IntegerValue } from "./type/integer_value";
import { StringValue } from "./type/string_value";
import { BooleanValue } from "./type/boolean_value";

console.log("start toy-db.");
const diskManager = new DiskManager();
const mustInitialize = !diskManager.existsDataFile();
if (mustInitialize) {
  console.log("create data file...");
  diskManager.createDataFile();
}
const bufferPoolManager = new BufferPoolManager(diskManager);
const catalog = new Catalog(bufferPoolManager);
if (mustInitialize) {
  console.log("initialize catalog...");
  catalog.initialize();
}

const rl = createInterface(process.stdin);
process.stdout.write("> ");
rl.on("line", (line) => {
  const tokens = line.trim().split(" ");
  if (tokens.length === 2 && tokens[0] === "select") {
    const tableHeap = catalog.getTableHeap(tokens[1]);
    console.log(tableHeap.schema);
    const tuples = tableHeap.scan();
    console.log(
      tuples
        .map((tuple) => tuple.values.map((value) => value.value).join(", "))
        .join("\n")
    );
  } else if (tokens.length === 3 && tokens[0] === "insert") {
    const tableHeap = catalog.getTableHeap(tokens[1]);
    const rawValues = tokens[2].split(",").map((value) => value.trim());
    const values = tableHeap.schema.columns.map((column, index) => {
      const value = rawValues[index];
      if (column.type === string2Type("INTEGER")) {
        return IntegerValue.of(value);
      } else if (column.type === string2Type("STRING")) {
        return StringValue.of(value);
      } else if (column.type === string2Type("BOOLEAN")) {
        return BooleanValue.of(value);
      } else {
        throw new Error("unknown type.");
      }
    });

    tableHeap.insertTuple(new Tuple(tableHeap.schema, values));
    console.log("inserted!");
  } else if (tokens.length === 1 && tokens[0] === "debug") {
    console.log(bufferPoolManager);
  } else if (tokens.length >= 3 && tokens[0] === "create") {
    const tableName = tokens[1];
    const columns = tokens.slice(2).map((token) => {
      const [name, type] = token.split(":");
      return new Column(name, string2Type(type));
    });
    const schema = new Schema(columns);
    catalog.createTable(tableName, schema);
    console.log("created!");
  } else if (tokens.length === 1 && tokens[0] === "exit") {
    bufferPoolManager.flushAllPages();
    console.log("bye!");
    process.exit(0);
  } else {
    console.log("invalid query.");
  }
  process.stdout.write("> ");
});
