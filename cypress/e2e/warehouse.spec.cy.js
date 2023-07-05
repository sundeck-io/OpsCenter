import { checkNoErrorOnThePage, setup } from "../support/utils";

describe("Warehouse section", () => {
  before(() => {
    setup();
  });

  it("works as expected", () => {
    // ---------------------------------------------
    // --- Menu: Warehouses (Warehouse Activity) ---
    // ---------------------------------------------
    cy.visit("/");

    const stringList= ["365", "90", "30", "7"];

    cy.get("span")
      .contains("Warehouses")
      .should("be.visible")
      .click();
    cy.get("span")
      .contains("Warehouse Activity")
      .should("be.visible");

    // Click on Filters
    cy.get('div[data-testid="stMarkdownContainer"]')
      .contains("Filters")
      .click();

    for (const str of stringList) {
      cy.get('button[kind="secondary"]')
        .contains(str)
        .click();

      // TODO: verify page finished loading

      checkNoErrorOnThePage();
    }

    // ----------------------------------
    // --- Menu: Warehouses (Heatmap) ---
    // ----------------------------------

    // Click on the Menu on the SideNav
    cy.get("span")
      .contains("Warehouses")
      .should("be.visible")
      .click();

    // Check that text "Warehouse Activity" is visible
    cy.get("span")
      .contains("Warehouse Activity")
      .should("be.visible");

    // Dropdown menu: clicks on the first dropdown found on the page
    cy.get('svg[title="open"]')
      .first()
      .should("be.visible")
      .click();

    cy.get('li[role="option"]')
      .should("be.visible")
      .contains("Warehouse Heatmap")
      .should("be.visible")
      .click();

    for (const str of stringList) {
      cy.get('button[kind="secondary"]')
        .contains(str)
        .click();

      // TODO: verify page finished loading

      checkNoErrorOnThePage();
    }

  });
});
