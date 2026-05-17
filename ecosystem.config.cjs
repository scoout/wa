module.exports = {
  apps: [
    {
      name: "whatsapp-bot",
      script: "./server.ts",
      interpreter: "node",
      interpreter_args: "--import tsx",
      env: {
        NODE_ENV: "production",
        TZ: "Asia/Jakarta"
      }
    }
  ]
};
