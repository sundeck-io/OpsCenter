import { checkNoErrorOnThePage,
         checkSuccessAlert,
         fillInTheSettingsConfigForm,
         buttonOnTabClick,
         buttonClick,
         checkForLoading,
         clickCheck,
         setup,
} from "../support/utils";

describe("Settings section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/");
  });

  it("Menu: Settings. Tab: Config. Validate that we can set and save credits values.", () => {

    cy.visit("/");
    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Settings" });

    clickCheck({ clickElem: 'button[role="tab"]', contains: "Config" });
    fillInTheSettingsConfigForm(10.00, 20.00, 30.00);
    buttonOnTabClick("Save");
    checkSuccessAlert("Saved");
  });

  it("Menu: Settings. Tab: Reset. Validate that we can click 'Reload' button and no exception is thrown .", () => {
    cy.visit("/");
    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Settings" });
    clickCheck({ clickElem: 'button[role="tab"]', contains: "Reset" });

    buttonClick("Reset and reload query history and warehouse events.");
    checkNoErrorOnThePage();

    checkSuccessAlert("Reset Complete.");
  });

  describe("Menu: Settings. Tab: Initial Setup", () => {
    it.skip("First step should be marked as completed", () => {
      cy.get("span")
        .contains("Settings")
        .should("be.visible")
        .click();

      cy.get('button[role="tab"]')
        .should("exist")
        .contains("Initial Setup")
        .should("exist")
        .click()

      cy.get("p")
        .contains("Step 1: Grant Snowflake Privileges [Completed]")
        .should("exist");
    });

    it.skip("Sundeck link should be present and contain correct information", () => {
      cy.get("span")
        .contains("Settings")
        .should("be.visible")
        .click();

      cy.get('button[role="tab"]')
        .should("exist")
        .contains("Initial Setup")
        .should("exist")
        .click()

      cy.get("a")
        .contains("right click here")
        .should("exist")
        .then(($a) => {
          const href = $a.prop("href");
          expect(href.startsWith("https://sundeck.io/try?source=opscenter&state=")).to.be.true;

          const url = new URL(href);
          const state = url.searchParams.get("state");

          // ensure state includes our snowflake account
          const payload = JSON.parse(atob(state));
          expect(payload.sf_account).to.equal(Cypress.env("SNOWFLAKE_ACCOUNT"));
        });
    });
  });
});
