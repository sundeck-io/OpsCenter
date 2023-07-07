import { setup } from "../support/utils";

describe("Home", () => {
  before(() => {
    setup();
  });

  it("the UI is available", () => {
    cy.visit("/");

    cy.get("h1", {timeout: 20000})
      .should("contain", "Welcome To Sundeck OpsCenter");
  });
});
