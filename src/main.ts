import { createInterface } from "readline";
import { TablePage } from "./storage/page/table_page";
import { DiskManager } from "./storage/disk/disk_manager";
import { BufferPoolManager } from "./buffer/buffer_pool_manager";

console.log("start toy-db.");
const diskManager = new DiskManager();
if (diskManager.init()) {
  console.log("data file created.");
  diskManager.writePage(new TablePage(0));
}
const bufferPoolManager = new BufferPoolManager(diskManager);

const rl = createInterface(process.stdin);
process.stdout.write("> ");
rl.on("line", (line) => {
  const tokens = line.trim().split(" ");
  if (tokens.length === 1 && tokens[0] === "select") {
    const page = bufferPoolManager.fetchPage(0);
    console.log(page);
    bufferPoolManager.unpinPage(0, false);
  } else if (tokens.length === 2 && tokens[0] === "insert") {
    const page = bufferPoolManager.fetchPage(0) as TablePage;
    if (page != null) {
      page.insertTuple({ value: parseInt(tokens[1]) });
      console.log("inserted!");
    }
    bufferPoolManager.unpinPage(0, true);
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
