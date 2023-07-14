const { defineConfig } = require("cypress");

module.exports = defineConfig({
  env: {
    OPSCENTER_URL: "http://localhost:8501",
    SNOWFLAKE_ACCOUNT: "",
  },
  e2e: {
    supportFile: false,
    defaultCommandTimeout: 20000
  },
});
