import {
  checkNoErrorOnThePage,
  dropDownOpen,
  dropDownElementClick,
  setup,
  checkForLoading,
  clickCheck,
} from "../support/utils";

describe("Queries section", () => {
  before(() => {
    setup();
  });

  it("Menu: Queries (Top Spenders)", () => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Queries" });

    dropDownOpen("Select Report");
    dropDownElementClick("Top Spenders");

    // List display option
    dropDownOpen("Pick View");
    dropDownElementClick("List");

    // Graph display option
    dropDownOpen("Pick View");
    dropDownElementClick("Graph");

    // Iterate over filters

    const stringList = ["365", "90", "30", "7"];

    cy.get("span").contains("Top Spenders").should("be.visible");

    // Click on Filters
    clickCheck({
      clickElem: 'div[data-testid="stMarkdownContainer"]',
      contains: "Filters",
    });

    for (const str of stringList) {
      clickCheck({ clickElem: 'button[kind="secondary"]', contains: str });
    }
  });

  it("Menu: Queries (dbt Summary)", () => {
    cy.visit("/");

    clickCheck({ clickElem: "span", contains: "Queries" });

    dropDownOpen("Select Report");
    dropDownElementClick("dbt Summary");
    checkNoErrorOnThePage();

    dropDownOpen("Pick View");
    dropDownElementClick("List");
    checkNoErrorOnThePage();

    // Iterate over filters

    const stringList = ["365", "90", "30", "7"];

    cy.get("span").contains("dbt Summary").should("be.visible");

    // Click on Filters
    clickCheck({
      clickElem: 'div[data-testid="stMarkdownContainer"]',
      contains: "Filters",
    });

    for (const str of stringList) {
      clickCheck({ clickElem: 'button[kind="secondary"]', contains: str });
    }
  });

  it("Menu: Queries (Query Activity)", () => {
    cy.visit("/");

    clickCheck({ clickElem: "span", contains: "Queries" });

    // Loop over category dropdown list, check no error on the page
    const categorylList = [
      "User",
      "Warehouse",
      "Role",
      "Query Type",
      "Execution Status",
    ];
    for (const category of categorylList) {
      dropDownOpen("Color by Category or Grouping Label");
      dropDownElementClick(category);

      // Test with Graph and List display options
      dropDownOpen("Pick View");
      dropDownElementClick("List");

      dropDownOpen("Pick View");
      dropDownElementClick("Graph");
    }

    // Iterate over filters

    const stringList = ["365", "90", "30", "7"];

    cy.get("span").contains("Query Activity").should("be.visible");

    // Click on Filters
    clickCheck({
      clickElem: 'div[data-testid="stMarkdownContainer"]',
      contains: "Filters",
    });

    for (const str of stringList) {
      clickCheck({ clickElem: 'button[kind="secondary"]', contains: str });
    }
  });
});
