import { checkNoErrorOnThePage, setup, fillInProbeForm, buttonClick, buttonCheckExists, generateUniqueName, probeDelete } from "../support/utils";

describe("Probes section", () => {
  before(() => {
    setup();
  });

  it("Menu: Probes", () => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Probes" });

    cy.get("span")
      .contains("Query Probes")
      .should("be.visible");
    checkNoErrorOnThePage();

    // Test #1: validate that clicking on "New" button starts page without error
    buttonClick("New");

    // Test #2: validate that clicking on "Cancel" brings form back to "New" button
    buttonClick("Cancel");
    buttonCheckExists("New");
    checkNoErrorOnThePage();

    // Test #3: Fill the form with valid values and save
    buttonClick("New");
    const probe_1 = generateUniqueName("probe");
    fillInProbeForm(
      probe_1,
      "query_text='%tpch_sf100%'",
      true,
      true,
      "vicky@sundeck.io, jinfeng@sundeck.io"
    );
    buttonClick("Create");

    cy.get("span")
      .contains("Query Probes", { timeout: 30000 })
      .scrollIntoView()
      .should("be.visible");
    checkNoErrorOnThePage();

    probeDelete(probe_1);

    // Test #4: Fill the form with valid values and save (checkboxes are unchecked)
    buttonClick("New");
    checkNoErrorOnThePage();
    const probe_2 = generateUniqueName("probe");
    fillInProbeForm(
      probe_2,
      "query_text='%tpch_sf1%'",
      false,
      false,
      "vicky@sundeck.io, jinfeng@sundeck.io"
    );
    buttonClick("Create");

    cy.get("span")
      .contains("Query Probes", { timeout: 30000 })
      .scrollIntoView()
      .should("be.visible");
    checkNoErrorOnThePage();

    probeDelete(probe_2);

    // Among other things, "New" button should exist
    buttonCheckExists("New");
  });
});
