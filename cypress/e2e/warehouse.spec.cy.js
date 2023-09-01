import { checkNoErrorOnThePage, setup } from "../support/utils";

describe("Warehouse section", () => {
  before(() => {
    setup();
  });

  it("Menu: Warehouses (Warehouse Activity)", () => {

    cy.visit("/");

    cy.wait(10000);

    const stringList= ["365", "90", "30", "7"];

    cy.get("span")
      .contains("Warehouses")
      .should("be.visible")
      .click();

    // Check that text "Warehouse Heatmap" is visible
    cy.get("span")
        .contains("Warehouse Heatmap")
        .should("be.visible");

    // Dropdown menu: clicks on the first dropdown found on the page
    cy.get('svg[title="open"]')
        .first()
        .should("be.visible")
        .click();

    cy.get('li[role="option"]')
        .should("be.visible")
        .contains("Warehouse Activity")
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

  // Test is skipped due to a known issue: exception while iterating over ranges
  // pandas/core/indexes/range.py", line 349, in get_loc
  it.skip("Menu: Warehouses (Heatmap)", () => {

    cy.visit("/");

    const stringList= ["365", "90", "30", "7"];

    // Heatmap should be visible
    cy.get("span")
      .contains("Warehouse Heatmap")
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

});
