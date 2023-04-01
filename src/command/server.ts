import { Instance } from "../common/instance";
import Table from "cli-table";
import { createServer } from "http";
import { exit } from "process";

const instance = new Instance();
createServer((req, res) => {
  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
  });
  req.on("end", async () => {
    try {
      body = body.trim();
      if (body === "exit" || body === "quit") {
        instance.shutdown();
        res.end("Bye");
        exit(0);
      }
      const result = await instance.executeSQL(body);
      if (result.length === 0) {
        res.end("Empty set");
        return;
      }
      const schema = result[0].schema;
      const table = new Table({
        head: schema.columns.map((column) => column.name),
      });
      table.push(
        ...result.map((row) =>
          row.values.map((value) => value.value.toString())
        )
      );
      res.end(table.toString());
    } catch (e) {
      res.end(e);
    }
  });
}).listen(8080);
console.log("Server started at http://localhost:8080");
