import {
  checkNoErrorOnThePage,
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
    fillInTheSettingsConfigForm({
      computeCreditCost: "10.0",
      serverlessCreditCost: "20.0",
      storageCost: "30.0",
    });
    buttonOnTabClick("Save");
    checkSuccessAlert("Saved");
  });

  it.skip("Menu: Settings. Tab: Reset. Validate that we can click 'Reload' button and no exception is thrown .", () => {
    cy.visit("/");
    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Settings" });
    clickCheck({ clickElem: 'button[role="tab"]', contains: "Reset" });

    buttonClick("Reset and reload query history and warehouse events.");
    checkNoErrorOnThePage();

    checkSuccessAlert("Reset Complete.");
  });
});
