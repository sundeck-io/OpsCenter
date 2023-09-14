import { setup } from "../../support/setupUtils";

describe("Home", () => {
  before(() => {
    setup();
  });

  it("the UI is available", () => {
    cy.visit("/");

    cy.get("h1").should("contain", "Welcome To Sundeck OpsCenter");
  });
});
