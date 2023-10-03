const { defineConfig } = require("cypress");
const snowflake = require("snowflake-sdk");
const {
  deleteProbes,
  createProbe,
  deleteLabels,
  createLabel,
} = require("./cypress/support/taskUtils");
const fs = require("fs");

module.exports = defineConfig({
  env: {
    OPSCENTER_URL: "http://localhost:8501",
    // change values below to run locally
    SNOWFLAKE_ACCOUNT: process.env.SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USERNAME: process.env.SNOWFLAKE_USERNAME,
    SNOWFLAKE_PASSWORD: process.env.SNOWFLAKE_PASSWORD,
    OPSCENTER_DATABASE: process.env.OPSCENTER_DATABASE,
  },
  e2e: {
    supportFile: false,
    defaultCommandTimeout: 40000,
    watchForFileChanges: false,
    experimentalStudio: true,
    experimentalRunAllSpecs: true,
    viewportHeight: 1080,
    viewportWidth: 1920,
    supportFile: "cypress/support/customCypressUtils.ts",
    video: true,
    videoCompression: 16,
    setupNodeEvents(on, config) {
      on("task", {
        deleteProbes: (args) => deleteProbes(config, args),
        createProbe: (args) => createProbe(config, args),
        deleteLabels: (args) => deleteLabels(config, args),
        createLabel: (args) => createLabel(config, args),
      });
    },
  },
  retries: {
    // Configure retry attempts for `cypress run`
    runMode: 2,
    // Configure retry attempts for `cypress open`
    openMode: 0,
  },
});
