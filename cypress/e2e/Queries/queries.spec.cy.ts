import { clickCheck, dropDownElementClick } from "../../support/clickUtils";
import { dropDownOpen } from "../../support/formUtils";
import { checkForLoading } from "../../support/loadingUtils";
import { setup } from "../../support/setupUtils";
import { MENU_TEXT } from "../../support/testConstants";

describe("Queries section", () => {
  before(() => {
    setup();
  });

  it("Menu: Queries (Top Spenders)", () => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: MENU_TEXT.QUERIES });

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

    clickCheck({ clickElem: "span", contains: MENU_TEXT.QUERIES });

    dropDownOpen("Select Report");
    dropDownElementClick("dbt Summary");

    dropDownOpen("Pick View");
    dropDownElementClick("List");

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

    clickCheck({ clickElem: "span", contains: MENU_TEXT.QUERIES });

    // Loop over category dropdown list, check no error on the page
    const categorylList = [
      "User",
      "Warehouse",
      "Role",
      "Query Type",
      "Execution Status",
    ];
    for (const category of categorylList) {
      checkForLoading();

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

    cy.get("span")
      .contains("Query Activity", { timeout: 40000 })
      .should("be.visible");

    // Click on Filters
    cy.log("Click on Filters");
    clickCheck({
      clickElem: 'div[data-testid="stMarkdownContainer"]',
      contains: "Filters",
    });

    for (const str of stringList) {
      clickCheck({ clickElem: 'button[kind="secondary"]', contains: str });
    }
  });
});
