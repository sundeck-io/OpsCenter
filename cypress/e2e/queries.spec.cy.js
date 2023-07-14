import { checkNoErrorOnThePage, setup } from "../support/utils";

describe("Queries section", () => {
  before(() => {
    setup();
  });

  it("works as expected", () => {
    // -------------------------------
    // --- Menu: Queries (dbt Summary)
    // -------------------------------

    cy.visit("/");

    cy.get("span")
      .contains("Queries")
      .should("be.visible")
      .click();

    cy.get("div")
      .contains("Query Activity")
      .should("be.visible")
      .click();

    // Dropdown menu: clicks on the first dropdown found on the page
    cy.get('svg[title="open"]')
      .first()
      .should("be.visible")
      .click();

    cy.get('li[role="option"]')
      .should("be.visible")
      .contains("dbt Summary")
      .should("be.visible")
      .click();

    checkNoErrorOnThePage();
  });
});
