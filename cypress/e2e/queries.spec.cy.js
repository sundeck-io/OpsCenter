import { checkNoErrorOnThePage, dropDownOpen, dropDownElementClick, setup } from "../support/utils";

describe("Queries section", () => {
  before(() => {
    setup();
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
    });
});
