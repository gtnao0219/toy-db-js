const path = require("path");

module.exports = {
  mode: "development",
  target: "node",
  entry: {
    server: path.resolve(__dirname, "src", "command", "server.ts"),
    client: path.resolve(__dirname, "src", "command", "client.ts"),
  },
  output: {
    filename: "[name].js",
    path: path.resolve(__dirname, "dist"),
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: "ts-loader",
      },
    ],
  },
  resolve: {
    extensions: [".ts", ".js"],
  },
};
