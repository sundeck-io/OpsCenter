import { checkNoErrorOnThePage, dropDownOpen, dropDownElementClick, setup } from "../support/utils";

describe("Queries section", () => {
  before(() => {
    setup();
  });


it("Menu: Queries (Top Spenders)", () => {
    cy.visit("/");

    cy.wait(10000);

    cy.get("span")
      .contains("Queries")
      .should("be.visible")
      .click();

    dropDownOpen("Select Report");
    dropDownElementClick("Top Spenders");
    checkNoErrorOnThePage();

    // List display option
    dropDownOpen("Pick View");
    dropDownElementClick("List");
    checkNoErrorOnThePage();

    // Graph display option
    dropDownOpen("Pick View");
    dropDownElementClick("Graph");
    checkNoErrorOnThePage();

    // Iterate over filters

    const stringList= ["365", "90", "30", "7"];

    cy.get("span")
      .contains("Top Spenders")
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

it("Menu: Queries (dbt Summary)", () => {
    cy.visit("/");

    cy.get("span")
      .contains("Queries")
      .should("be.visible")
      .click();

    dropDownOpen("Select Report");
    dropDownElementClick("dbt Summary");
    checkNoErrorOnThePage();

    dropDownOpen("Pick View");
    dropDownElementClick("List");
    checkNoErrorOnThePage();

    // Iterate over filters

    const stringList= ["365", "90", "30", "7"];

    cy.get("span")
      .contains("dbt Summary")
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

it("Menu: Queries (Query Activity)", () => {
    cy.visit("/");

    cy.get("span")
      .contains("Queries")
      .should("be.visible")
      .click();

    // Loop over category dropdown list, check no error on the page
    const categorylList= ["User", "Warehouse", "Role", "Query Type", "Execution Status"];
    for (const category of categorylList) {

      dropDownOpen("Color by Category or Grouping Label");
      dropDownElementClick(category);
      checkNoErrorOnThePage();

      // Test with Graph and List display options
      dropDownOpen("Pick View");
      dropDownElementClick("List");
      checkNoErrorOnThePage();

      dropDownOpen("Pick View");
      dropDownElementClick("Graph");
      checkNoErrorOnThePage();
    }

    // Iterate over filters

    const stringList= ["365", "90", "30", "7"];

    cy.get("span")
      .contains("Query Activity")
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
