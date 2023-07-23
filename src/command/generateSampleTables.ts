import { Instance } from "../common/instance";
import { DiskManagerImpl } from "../storage/disk/disk_manager";

(async () => {
  const diskManager = new DiskManagerImpl();
  await diskManager.reset();

  const instance = new Instance();
  await instance.bootstrap();

  const accountsTableCreateSQL = `
    CREATE TABLE accounts(
      id INTEGER,
      name VARCHAR
    );
  `;
  const usersTableCreateSQL = `
    CREATE TABLE users(
      id INTEGER,
      accountId INTEGER,
      name VARCHAR,
      archived BOOLEAN
    );
  `;
  const accountsIdIndexCreateSQL = `
    CREATE INDEX accounts_id_idx ON accounts(id);
  `;
  const usersIdIndexCreateSQL = `
    CREATE INDEX users_id_idx ON users(id);
  `;
  const usersAccountIdIndexCreateSQL = `
    CREATE INDEX users_accountId_idx ON users(accountId);
  `;
  await instance.executeSQL(accountsTableCreateSQL, null);
  await instance.executeSQL(usersTableCreateSQL, null);

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
  const { transactionId } = await instance.executeSQL("BEGIN;", null);
  for (let i = 0; i < accounts.length; ++i) {
    const account = accounts[i];
    const sql = `INSERT INTO accounts VALUES (${account[0]}, '${account[1]}')`;
    await instance.executeSQL(sql, transactionId);
  }
  for (let i = 0; i < users.length; ++i) {
    const user = users[i];
    const sql = `INSERT INTO users VALUES (${user[0]}, ${user[1]}, '${user[2]}', ${user[3]})`;
    await instance.executeSQL(sql, transactionId);
  }

  await instance.executeSQL(accountsIdIndexCreateSQL, null);
  await instance.executeSQL(usersIdIndexCreateSQL, null);
  await instance.executeSQL(usersAccountIdIndexCreateSQL, null);

  await instance.executeSQL("COMMIT;", transactionId);
  await instance.shutdown();
})();
