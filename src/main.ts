import { createInterface } from "readline";
import { initDataFile, readPage, writePage } from "./disk";
import {
  createTablePage,
  deserializeTablePage,
  insertTuple,
  serializeTablePage,
} from "./table_page";

console.log("start");
if (initDataFile()) {
  console.log("data file created");
  const tablePage = createTablePage();
  writePage(0, serializeTablePage(tablePage));
}

const rl = createInterface(process.stdin);
process.stdout.write("> ");
rl.on("line", (line) => {
  const tokens = line.trim().split(" ");
  if (tokens.length === 1 && tokens[0] === "select") {
    const tablePage = deserializeTablePage(readPage(0));
    console.log(tablePage);
  } else if (tokens.length === 2 && tokens[0] === "insert") {
    const tablePage = deserializeTablePage(readPage(0));
    insertTuple(tablePage, { value: parseInt(tokens[1]) });
    writePage(0, serializeTablePage(tablePage));
    console.log("inserted!");
  } else if (tokens.length === 1 && tokens[0] === "debug") {
  } else {
    console.log("invalid query");
  }
  process.stdout.write("> ");
});
