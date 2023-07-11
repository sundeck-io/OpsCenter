import { checkNoErrorOnThePage,
         checkSuccessAlert,
         fillInTheSettingsConfigForm,
         buttonOnTabClick,
         buttonClick,
         setup } from "../support/utils";

describe("Settings section", () => {
  before(() => {
    setup();
  });

  it("Menu: Settings. Tab: Config. Validate that we can set and save credits values.", () => {
    cy.visit("/");

    // We have a bug with Tasks menu were we get exception below
    // IndexError: index 0 is out of bounds for axis 0 with size 0
    // Ignoring this for now as this is a default page we land on
    // when clicking on Settings
    cy.wait(5000);

    cy.get("span", {timeout: 20000})
      .contains("Settings")
      .should("be.visible")
      .click();

    cy.wait(5000);

    cy.get('button[role="tab"]', {timeout: 20000})
      .should("exist")
      .contains("Config")
      .should("exist")
      .click()

    fillInTheSettingsConfigForm(10.00, 20.00, 30.00);
    buttonOnTabClick("Save");
    checkNoErrorOnThePage();
    checkSuccessAlert("Saved");

  });

  it("Menu: Settings. Tab: Reset. Validate that we can click 'Reload' button and no exception is thrown .", () => {
    cy.visit("/");

    cy.wait(2000);

    cy.get("span", {timeout: 20000})
      .contains("Settings")
      .should("be.visible")
      .click();

    cy.get('button[role="tab"]', {timeout: 20000})
      .should("exist")
      .contains("Reset")
      .should("exist")
      .click()

    buttonClick("Reset and reload query history and warehouse events.");
    checkNoErrorOnThePage();

    // TODO: this wait is temporary until we figure out how to wait in cypress for element to disappear
    cy.wait(30000);
    checkSuccessAlert("Reset Complete.");

  });

});
