import { checkNoErrorOnThePage,
         setup,
         checkForLoading,
         clickCheck, }
from "../support/utils";

describe("Warehouse section", () => {
  before(() => {
    setup();
  });

  it("Menu: Warehouses (Warehouse Activity)", () => {

    cy.visit("/");

    checkForLoading();

    const stringList= ["365", "90", "30", "7"];

    clickCheck({ clickElem: "span", contains: "Warehouses" });

    // Check that text "Warehouse Heatmap" is visible
    cy.get("span")
        .contains("Warehouse Heatmap")
        .should("be.visible");

    // Dropdown menu: clicks on the first dropdown found on the page
    cy.get('svg[title="open"]')
        .first()
        .should("be.visible")
        .click();

    clickCheck({ clickElem: 'li[role="option"]', contains: "Warehouse Activity" });

    // Check that text "Warehouse Activity" is visible
    cy.get("span")
        .contains("Warehouse Activity")
        .should("be.visible");

    // Click on Filters
    clickCheck({ clickElem: 'div[data-testid="stMarkdownContainer', contains: "Filters" });

    for (const str of stringList) {
      clickCheck({ clickElem: 'button[kind="secondary"]', contains: str });
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
