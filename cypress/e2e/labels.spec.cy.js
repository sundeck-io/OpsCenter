import { checkNoErrorOnThePage,
         setup,
         buttonClick,
         buttonCheckExists,
         generateUniqueName,
         fillInNewGroupedLabelForm,
         addNewLabelToGroup,
         labelDelete,
         labelUpdateClick,
         updateUngroupedLabelForm,
         checkGroupLabelNotExist,
         checkFailureAlert,
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
      labelDelete(
        groupName,
        label
      );
    }

   // Issue #80: function is not doing the right thing
   // checkGroupLabelNotExist(groupName);
   checkNoErrorOnThePage();


  }); // end group labels test

  it.skip("Menu: Labels. Create/Delete ungrouped labels", () => {

    const label_1 = generateUniqueName("label");
    const label_2 = generateUniqueName("label");
    const labelList= [ label_1, label_2 ];

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();

    // Fill the form for Ungrouped label with valid values and save
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_1,
      "compilation_time > 5000"
     );

    buttonClick("Create");
    checkNoErrorOnThePage();

    // Add one more ungrouped label
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_2,
      "compilation_time > 5000"
     );

    buttonClick("Create");
    checkNoErrorOnThePage();

    // Delete all the labels that were created in this test
    for (const label of labelList) {
      labelDelete(
        "Ungrouped",
        label
      );
    }

  }); // end ungrouped labels test


  it.skip("Menu: Labels. Update ungrouped label: positive test cases", () => {

    const label_1 = generateUniqueName("label");
    const label_2 = generateUniqueName("label");
    const labelList= [ label_2 ];

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();

    // Fill the form for ungrouped label with valid values and save
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_1,
      "query_type = 'delete'"
     );

    // create label_1
    buttonClick("Create");
    checkNoErrorOnThePage();

    // Test #1 update label: change name from label_1 to label_2
    // Should run successfully because there is no other label with name label_2
    labelUpdateClick("Ungrouped", label_1);

    updateUngroupedLabelForm(
      label_1,
      label_2,
      "None"
     );

    // save updated label name: should succeed
    buttonClick("Update");
    checkNoErrorOnThePage();

    // Test #2 update label: change label_2 condition to another valid condition
    labelUpdateClick("Ungrouped", label_2);

    // update only label condition for label_2
    updateUngroupedLabelForm(
      label_2,
      "None",
      "query_type = 'call'"
     );

    // save updated label name: should succeed
    buttonClick("Update");
    checkNoErrorOnThePage();

    // Test #3: change both label name and condition

    labelUpdateClick("Ungrouped", label_2); // clicks on the pencil for label with the name label_2

    updateUngroupedLabelForm(
      label_2,
      label_1,
      "compilation_time > 10000"
     );

    // save updated label name: should succeed
    buttonClick("Update");
    checkNoErrorOnThePage();

    labelDelete("Ungrouped", label_1);

  });

  it.skip("Menu: Labels. Update ungrouped label: negative cases (error conditions)", () => {

    const label_1 = generateUniqueName("label");
    const label_2 = generateUniqueName("label");
    const labelList= [ label_1, label_2 ];

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();

    // Test #1: update label name with the ungrouped label name that already exists

    // Create two ungrouped labels with unique names

    // Fill the form for ungrouped label with valid values and save
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_1,
      "query_type = 'delete'"
     );

    // create label_1
    buttonClick("Create");
    checkNoErrorOnThePage();

    // Fill the form for ungrouped label with valid values and save
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_2,
      "query_type = 'truncate'"
     );

    // create label_2
    buttonClick("Create");
    checkNoErrorOnThePage();

    // Test #1 update label: change name of label_1 to label_2
    // Should fail with the error that label with this name already exists

    labelUpdateClick("Ungrouped", label_1);

    updateUngroupedLabelForm(
      label_1,
      label_2,
      "None"
     );

    // save updated label name: should fail as label already exists.
    buttonClick("Update");
    checkNoErrorOnThePage();
    checkFailureAlert("A label with this name already exists. Please choose a distinct name.");

    // Click on the update button again and confirm that we still got the same error.
    buttonClick("Update");
    checkNoErrorOnThePage();
    checkFailureAlert("A label with this name already exists. Please choose a distinct name.");

    // Should bring us back to the label list
    buttonClick("Cancel");

    // TODO: need to figure out why element can't be found without reloading
    cy.reload();
    cy.wait(5000);

    // Test #2 update label: change label_1 condition to an invalid condition
    labelUpdateClick("Ungrouped", label_1);

    // update only label condition for label_1
    updateUngroupedLabelForm(
      label_1,
      "None",
      "compile_time > 5000"
     );

    // save updated label name: should fail
    buttonClick("Update");
    checkNoErrorOnThePage();
    checkFailureAlert("Invalid condition SQL. Please check your syntax.SQL compilation error: error line 2 at position 0\ninvalid identifier 'COMPILE_TIME'");

    // Click on the update button again and confirm that we still got the same error.
    buttonClick("Update");
    checkNoErrorOnThePage();
    checkFailureAlert("Invalid condition SQL. Please check your syntax.SQL compilation error: error line 2 at position 0\ninvalid identifier 'COMPILE_TIME'");

    // Should bring us to the label list
    buttonClick("Cancel");

    // TODO: need to figure out why element can't be found without reloading
    cy.reload();
    cy.wait(5000);

    // Delete all the labels that were created in this test
    for (const label of labelList) {
      labelDelete(
        "Ungrouped",
        label
      );
    }

  });

  it.skip("Menu: Labels. Create label with already existing label name: negative cases (error conditions)", () => {

    const label_1 = generateUniqueName("label");
    const group_1 = generateUniqueName("group");

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible");
    checkNoErrorOnThePage();


    // Setup test: create ungrouped label

    // Fill the form for ungrouped label with valid values and save
    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_1,
      "query_type = 'insert'"
     );

    // create label_1
    buttonClick("Create");
    checkNoErrorOnThePage();

    // Test #1: create ungrouped label with the same name as above.
    // Should fail with the error that label with this name already exists

    buttonClick("New");
    checkNoErrorOnThePage();
    fillInNewUngroupedLabelForm(
      label_1,
      "query_type = 'insert-should-fail'"
     );

    buttonClick("Create");
    checkFailureAlert("Duplicate label name found. Please use a distinct name.");

    // Should bring us to the label list
    buttonClick("Cancel");

    // TODO: need to figure out why element can't be found without reloading
    cy.reload();
    cy.wait(5000);

    // Test #2: create grouped label with the same name as above.
    // Should fail with the error that label with this name already exists

    buttonClick("New (in group)");
    checkNoErrorOnThePage();
    fillInNewGroupedLabelForm(
      group_1,
      label_1,
      "compilation_time > 5000",
      100
     );

    buttonClick("Create");
    checkFailureAlert("Duplicate label name found. Please use a distinct name.");

    // Should bring us to the label list
    buttonClick("Cancel");

    // TODO: need to figure out why element can't be found without reloading
    cy.reload();
    cy.wait(5000);

    // Cleanup: delete all the labels that were created in this test
      labelDelete( "Ungrouped", label_1 );
  });
});
