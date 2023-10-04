const { defineConfig } = require("cypress");
const fs = require("fs");

module.exports = defineConfig({
  env: {
    OPSCENTER_URL: "http://localhost:8501",
    SNOWFLAKE_ACCOUNT: "",
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
      // https://docs.cypress.io/guides/guides/screenshots-and-videos#Delete-videos-for-specs-without-failing-or-retried-tests
      on("after:spec", (spec, results) => {
        if (results && results.video) {
          // Do we have failures for any retry attempts?
          const failures = results.tests.some((test) =>
            test.attempts.some((attempt) => attempt.state === "failed")
          );
          if (!failures) {
            // delete the video if the spec passed and no tests retried
            fs.unlinkSync(results.video);
          }
        }
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
