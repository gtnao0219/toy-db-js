import { createInterface } from "readline";
import { request } from "http";
import { Response, TRANSACTION_ID_HEADER_NAME } from "../common/common";

const rl = createInterface(process.stdin);
const prompt = "> ";
let transactionId: number | null = null;
process.stdout.write(prompt);
rl.on("line", async (line) => {
  request(
    {
      hostname: "localhost",
      port: 8080,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(transactionId == null
          ? {}
          : { [TRANSACTION_ID_HEADER_NAME]: transactionId }),
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
        transactionId = response.transactionId;
        process.stdout.write(prompt);
      });
    }
  ).end(line);
});
