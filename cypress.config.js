const { defineConfig } = require("cypress");

module.exports = defineConfig({
  env: {
    OPSCENTER_URL: "http://localhost:8501",
  },
  e2e: {
    supportFile: false
  },
});
