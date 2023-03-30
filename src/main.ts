import { createInterface } from "readline";
import { Instance } from "./common/instance";
import Table from "cli-table";

const instance = new Instance();
const rl = createInterface(process.stdin);
const prompt = "> ";
process.stdout.write(prompt);
rl.on("line", async (line) => {
  if (line.trim() === "exit" || line.trim() === "quit") {
    await instance.shutdown();
    process.exit(0);
  }
  try {
    const result = await instance.executeSQL(line);
    if (result.length === 0) {
      console.log("Empty set");
      process.stdout.write(prompt);
      return;
    }
    const schema = result[0].schema;
    const table = new Table({
      head: schema.columns.map((column) => column.name),
    });
    table.push(
      ...result.map((row) => row.values.map((value) => value.value.toString()))
    );
    console.log(table.toString());
  } catch (e) {
    console.log(e);
  }
  process.stdout.write(prompt);
});
