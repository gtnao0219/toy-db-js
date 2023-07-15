import { Instance } from "../common/instance";
import Table from "cli-table";
import { createServer } from "http";
import { exit } from "process";

type SessionStore = {
  [address: string]: {
    [port: number]: Session;
  };
};
type Session = {
  transactionId: number | null;
};

(async () => {
  const instance = new Instance();
  await instance.bootstrap();
  const sessionStore: SessionStore = {};

  createServer((req, res) => {
    const address = req.socket.remoteAddress;
    const port = req.socket.remotePort;
    if (address == null || port == null) {
      JSON.stringify({
        result: "unknown address",
      });
      return;
    }
    let session: Session | null = null;
    const storeByAddress = sessionStore[address];
    if (storeByAddress != null) {
      session = storeByAddress[port] ?? null;
    } else {
      sessionStore[address] = {};
      sessionStore[address][port] = { transactionId: null };
    }

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
            })
          );
          exit(0);
        }
        if (body === "debug") {
          console.log(JSON.stringify(instance, null, 2));
          res.end(
            JSON.stringify({
              result: "Debugging...",
            })
          );
        }
        const result = await instance.executeSQL(
          body,
          session?.transactionId ?? null
        );
        sessionStore[address][port] = {
          transactionId: result.transactionId,
        };
        const rows = result.rows;
        if (rows.length === 0) {
          res.end(
            JSON.stringify({
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
            result: table.toString(),
          })
        );
      } catch (e: any) {
        console.log(e.stack);
        res.end(
          JSON.stringify({
            result: e.message || "",
          })
        );
      }
    });
  }).listen(8080);
  console.log("Server started at http://localhost:8080");
})();
