import { Instance } from "../common/instance";
import Table from "cli-table";
import { createServer } from "http";
import { exit } from "process";
import { TRANSACTION_ID_HEADER_NAME } from "../common/common";

(async () => {
  const instance = new Instance();
  await instance.init();
  createServer((req, res) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", async () => {
      try {
        body = body.trim();
        if (body === "exit" || body === "quit" || body === "break") {
          if (body !== "break") {
            await instance.shutdown();
          }
          res.end(
            JSON.stringify({
              result: "Shutting down...",
              transactionId: null,
            })
          );
          exit(0);
        }
        if (body === "debug") {
          console.log(JSON.stringify(instance, null, 2));
          res.end(
            JSON.stringify({
              result: "Debugging...",
              transactionId: null,
            })
          );
        }
        const rawTransactionId = req.headers[TRANSACTION_ID_HEADER_NAME];
        const transactionId =
          rawTransactionId == null
            ? null
            : parseInt(
                Array.isArray(rawTransactionId)
                  ? rawTransactionId[0]
                  : rawTransactionId
              );
        if (transactionId != null && isNaN(transactionId)) {
          throw new Error("invalid transaction id");
        }
        const result = await instance.executeSQL(body, transactionId);
        const rows = result.rows;
        if (rows.length === 0) {
          res.end(
            JSON.stringify({
              transactionId: result.transactionId,
              result: "Empty set",
            })
          );
          return;
        }
        const schema = rows[0].schema;
        const table = new Table({
          head: schema.columns.map((column) => column.name),
        });
        table.push(
          ...rows.map((row) =>
            row.values.map((value) => value.value.toString())
          )
        );
        res.end(
          JSON.stringify({
            transactionId: result.transactionId,
            result: table.toString(),
          })
        );
      } catch (e: any) {
        console.log(e.stack);
        res.end(
          JSON.stringify({
            transactionId: null,
            result: e.message || "",
          })
        );
      }
    });
  }).listen(8080);
  console.log("Server started at http://localhost:8080");
})();
