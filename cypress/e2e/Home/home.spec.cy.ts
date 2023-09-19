import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";

describe("Home", () => {
  before(() => {
    setup();
  });

  it("the UI is available", () => {
    cy.visit("/");

    checkOnCorrectPage({ headerText: "Welcome To Sundeck OpsCenter" });
  });
});
