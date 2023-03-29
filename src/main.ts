import { createInterface } from "readline";
import { Instance } from "./common/instance";

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
    console.log(
      result
        .map((row) => row.values.map((value) => value.value).join(", "))
        .join("\n")
    );
  } catch (e) {
    console.log(e);
  }
  process.stdout.write(prompt);
});
