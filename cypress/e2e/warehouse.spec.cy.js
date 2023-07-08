import { checkNoErrorOnThePage, setup } from "../support/utils";

describe("Warehouse section", () => {
  before(() => {
    setup();
  });

  it("Menu: Warehouses (Warehouse Activity)", () => {

    cy.visit("/");

    const stringList= ["365", "90", "30", "7"];

    cy.get("span", {timeout: 20000})
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

      checkNoErrorOnThePage();
    }
  });

  // Test is skipped due to a known issue: exception while iterating over ranges
  // pandas/core/indexes/range.py", line 349, in get_loc
  it.skip("Menu: Warehouses (Heatmap)", () => {

    cy.visit("/");

    const stringList= ["365", "90", "30", "7"];

    // Click on the Menu on the SideNav
    cy.get("span", {timeout: 20000})
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

    // Click on Filters
    cy.get('div[data-testid="stMarkdownContainer"]')
      .contains("Filters")
      .click();

    for (const str of stringList) {
      cy.get('button[kind="secondary"]')
        .contains(str)
        .click();

      checkNoErrorOnThePage();
    }
  });

});
