import { Binder } from "../binder/binder";
import { BufferPoolManagerImpl } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { Column } from "../catalog/column";
import { Schema } from "../catalog/schema";
import { LockManager } from "../concurrency/lock_manager";
import { TransactionManager } from "../concurrency/transaction_manager";
import { ExecutorContext } from "../execution/executor_context";
import { ExecutorEngine } from "../execution/executor_engine";
import { PlanNode } from "../execution/plan";
import { plan } from "../execution/planner";
import { Parser } from "../parser/parser";
import { LogManager } from "../recovery/log_manager";
import { DiskManagerImpl } from "../storage/disk/disk_manager";
import { Type } from "../type/type";

(async () => {
  const diskManager = new DiskManagerImpl();
  await diskManager.reset();

  const bufferPoolManager = new BufferPoolManagerImpl(diskManager);
  const lockManager = new LockManager();
  const logManager = new LogManager(diskManager);
  const transactionManager = new TransactionManager(lockManager, logManager);
  const catalog = new Catalog(bufferPoolManager, logManager);

  const transaction1 = await transactionManager.begin();
  await catalog.initialize(transaction1);
  await transactionManager.commit(transaction1);
  await catalog.setupNextOid();

  const executorEngine = new ExecutorEngine(
    bufferPoolManager,
    transactionManager,
    catalog
  );
  const transaction2 = await transactionManager.begin();
  const executorContext = new ExecutorContext(
    transaction2,
    bufferPoolManager,
    lockManager,
    transactionManager,
    catalog
  );
  const accountsSchema = new Schema([
    new Column("id", Type.INTEGER),
    new Column("name", Type.VARCHAR),
  ]);
  const usersSchema = new Schema([
    new Column("id", Type.INTEGER),
    new Column("accountId", Type.INTEGER),
    new Column("name", Type.VARCHAR),
    new Column("archived", Type.BOOLEAN),
  ]);
  await catalog.createTable("accounts", accountsSchema, transaction2);
  await catalog.createTable("users", usersSchema, transaction2);
  await catalog.createIndex("accounts_id_idx", "accounts", "id", transaction2);
  await catalog.createIndex("users_id_idx", "users", "id", transaction2);
  await catalog.createIndex(
    "users_accountId_idx",
    "users",
    "accountId",
    transaction2
  );

  const accounts = [
    [1, "A company"],
    [2, "B company"],
    [3, "C company"],
    [4, "D company"],
    [5, "E company"],
    [6, "F company"],
    [7, "G company"],
    [8, "H company"],
    [9, "I company"],
    [10, "J company"],
  ];
  const users = [
    [1, 1, "Alice", false],
    [2, 1, "Bob", false],
    [3, 1, "Carol", false],
    [4, 1, "Dave", false],
    [5, 1, "Eve", false],
    [6, 2, "Frank", false],
    [7, 2, "Grace", false],
    [8, 2, "Helen", false],
    [9, 2, "Ivan", false],
    [10, 2, "John", true],
    [11, 3, "Karl", true],
    [12, 3, "Linda", true],
    [13, 3, "Mike", true],
    [14, 3, "Nancy", true],
    [15, 3, "Oliver", true],
    [16, 4, "Paul", false],
  ];
  for (let i = 0; i < accounts.length; ++i) {
    const account = accounts[i];
    const sql = `INSERT INTO accounts VALUES (${account[0]}, '${account[1]}')`;
    const plan = await generatePlan(sql, catalog);
    await executorEngine.execute(executorContext, plan);
  }
  for (let i = 0; i < users.length; ++i) {
    const user = users[i];
    const sql = `INSERT INTO users VALUES (${user[0]}, ${user[1]}, '${user[2]}', ${user[3]})`;
    const plan = await generatePlan(sql, catalog);
    await executorEngine.execute(executorContext, plan);
  }
  await transactionManager.commit(transaction2);
  await bufferPoolManager.flushAllPages();
})();

async function generatePlan(sql: string, catalog: Catalog): Promise<PlanNode> {
  const parser = new Parser(sql);
  const ast = parser.parse();
  const binder = new Binder(catalog);
  const statement = await binder.bind(ast);
  const planNode = plan(statement);
  return planNode;
}
