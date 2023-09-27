import { setup } from "../support/setupUtils";
import { clickCheck } from "../support/clickUtils";
import { checkInitialLoading } from "../support/loadingUtils";
import { MENU_TEXT } from "../support/testConstants";
import { checkNoErrorOnThePage } from "../support/alertUtils";

describe.skip("Fake test to replicate an error", () => {
  before(() => {
    setup();
  });

  it("trying to run into an indexError: be sure to turn off main segments of the loading utils", () => {
    cy.visit("/");

    checkInitialLoading();

    for (let i = 0; i < 100; i++) {
      for (let key in MENU_TEXT) {
        clickCheck({ clickElem: "span", contains: MENU_TEXT[key] });
        cy.wait(500);
        checkNoErrorOnThePage();
      }
    }
  });
});
