import { createInterface } from "readline";
import { Instance } from "./common/instance";

const instance = new Instance();
const rl = createInterface(process.stdin);
const prompt = "> ";
process.stdout.write(prompt);
rl.on("line", (line) => {
  if (line.trim() === "exit" || line.trim() === "quit") {
    instance.shutdown();
    process.exit(0);
  }
  try {
    const result = instance.executeSQL(line);
    console.log(result);
  } catch (e) {
    console.log(e);
  }
  process.stdout.write(prompt);
});
