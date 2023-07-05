const { defineConfig } = require("cypress");

module.exports = defineConfig({
  env: {
    "OPSCENTER_URL": "http://localhost:8501"
  },
  e2e: {
    setupNodeEvents(on, config) {
      // implement node event listeners here
    },
  },
});
