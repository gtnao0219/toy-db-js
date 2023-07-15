import { createInterface } from "readline";
import { request } from "http";
import { Response } from "../common/common";

const rl = createInterface(process.stdin);
const prompt = "> ";
process.stdout.write(prompt);
rl.on("line", async (line) => {
  request(
    {
      hostname: "localhost",
      port: 8080,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    },
    (res) => {
      let body = "";
      res.on("data", (chunk) => {
        body += chunk;
      });
      res.on("end", () => {
        const response: Response = JSON.parse(body);
        console.log(response.result);
        process.stdout.write(prompt);
      });
    }
  ).end(line);
});
