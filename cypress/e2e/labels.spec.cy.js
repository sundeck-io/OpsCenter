import { checkNoErrorOnThePage,
         setup,
         buttonClick,
         buttonCheckExists,
         generateUniqueName,
         fillInNewGroupedLabelForm,
         addNewLabelToGroup,
         groupLabelDelete,
         checkGroupLabelNotExist,
         fillInNewUngroupedLabelForm } from "../support/utils";

describe("Labels section", () => {
  before(() => {
    setup();
  });

  it("Menu: Labels. Validate that New/Create/Cancel buttons don't result in failure to load the page", () => {
    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();

    // Test #1: validate that clicking on "New" button starts page without error
    buttonClick("New");
    checkNoErrorOnThePage();

    // Test #2: validate that clicking on "Cancel" brings form back to "New" button
    buttonClick("Cancel");
    checkNoErrorOnThePage();
    buttonCheckExists("New");
    checkNoErrorOnThePage();

    // Test #3: validate that clicking on "New (in group)" button starts page without error
    buttonClick("New (in group)");
    checkNoErrorOnThePage();

    // Test #4: validate that clicking on "Cancel" brings form back to "New" button
    buttonClick("Cancel");
    checkNoErrorOnThePage();
    buttonCheckExists("New (in group)");
    checkNoErrorOnThePage();
  });

  it("Menu: Labels. Create/Delete grouped labels", () => {

    const groupName = generateUniqueName("Workload");
    const label_1 = generateUniqueName("label");
    const label_2 = generateUniqueName("label");
    const label_3 = generateUniqueName("label");
    const labelList= [ label_1, label_2, label_3];

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();


    // Test #1: Fill the form for Grouped label with valid values and save
    buttonClick("New (in group)");
    checkNoErrorOnThePage();
    fillInNewGroupedLabelForm(
      groupName,
      label_1,
      "compilation_time > 5000",
      100
     );
    buttonClick("Create");
    checkNoErrorOnThePage();
    //groupedLabelDelete(label_1);

    // Test #2: Add two more labels to grouped label
    addNewLabelToGroup(
      groupName,
      label_2,
      "query_type = 'select'",
      200
    );

    addNewLabelToGroup(
      groupName,
      label_3,
      "bytes_spilled_to_local_storage > 0",
      300
    );

    // Delete all the labels that were created in this test
    for (const label of labelList) {
      groupLabelDelete(
        groupName,
        label
      );
    }

   // Validate that group on the Labels page does not exist
   // Suspicious behavior. 1 out of 3 times fails because "Labels" menu brings us directly
   // to the "New Label" page. Disabled until fully investigated.
   // checkGroupLabelNotExist(groupName);
   checkNoErrorOnThePage();


  }); // end group labels test

});
