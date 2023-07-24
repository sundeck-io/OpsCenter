import { v4 as uuidv4 } from "uuid";

export function setup() {
  Cypress.config("baseUrl", Cypress.env("OPSCENTER_URL"));
}

export function checkNoErrorOnThePage() {
  // TODO: Temporarily for testing, make sure page loaded
  cy.wait(5000);

  // Validate no exception is thrown
  cy.get('div[class="stException"]').should("not.exist");
};

// Check for Success notification with particular text presence
export function checkSuccessAlert(notificationText) {
  cy.get('div[role="alert"][data-baseweb="notification"]', { timeout: 60000 })
    .and('contain', notificationText);
};

// Check for failure notification with particular text presence
export function checkFailureAlert(notificationText) {
  cy.get('div[role="alert"][data-baseweb="notification"]', { timeout: 60000 })
    .and('contain', notificationText);
};

export const fillInProbeForm = (
  probeName,
  condition,
  emailTheAuthor,
  cancelTheQuery,
  emailOthers
) => {

  cy.get('input[aria-label="Probe Name"]')
    .clear()
    .type(probeName);

  cy.get('textarea[aria-label="Condition"]')
    .clear()
    .type(condition);

  if (emailTheAuthor) {
    // check({force: true}) - explanation below
    // https://docs.cypress.io/guides/references/error-messages#cy-failed-because-the-element-cannot-be-interacted-with
    cy.get('input[aria-label="Email the author"]')
      .should("exist")
      .check({ force: true });
  }

  if (cancelTheQuery) {
    cy.get('input[aria-label="Cancel the query"]')
      .should("exist")
      .check({ force: true });
  }

  cy.get('textarea[aria-label="Email others (comma delimited)"]')
    .clear()
    .type(emailOthers);
};

export const buttonClick = (buttonName) => {
  cy.get('button[kind="secondary"]')
    .contains(buttonName)
    .click({force: true});
};

export const buttonOnTabClick = (buttonName) => {
  cy.get('button[kind="secondaryFormSubmit"]')
    .contains(buttonName)
    .click({force: true});
};

export const buttonCheckExists = (buttonName) => {
  cy.get('button[kind="secondary"]')
    .contains(buttonName)
    .should("exist");
};

function generateUUID(){
  const uuid = uuidv4();
  return uuid;
}

export function generateUniqueName(prefix){
  const uuid = generateUUID();
  const uniqueName = `${prefix}_${uuid}`;
  return uniqueName;
}

export const probeDelete = (probeName) => {
  cy.log("Deleting probe: ",  probeName);

  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(probeName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with probeName
    .should("exist")
    .within(() => {  // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        //.contains("&#x1F5D1;") // Does not work. This is unicode HTML representation of the wastebasket icon.
        .eq(-2) // Brute force solution: chose second before last column (wastebasket)
        .should("exist")
        .click()
    })
};

// If "None" - don't modify the field
export const fillInNewUngroupedLabelForm = (
  labelName,
  condition
) => {

  if ( labelName != "None" ) {
    cy.get('input[aria-label="Label Name"]')
      .clear()
      .type(labelName);
  }

  if ( condition != "None" ) {
    cy.get('textarea[aria-label="Condition"]')
      .clear()
      .type(condition).type('{command+enter}');
  }
};

export const fillInNewGroupedLabelForm = (
  groupName,
  labelName,
  condition,
  rank
) => {

  cy.get('input[aria-label="Group Name"]')
    .clear()
    .type(groupName);

  cy.get('input[aria-label="Label Name"]')
    .clear()
    .type(labelName);

  cy.get('textarea[aria-label="Condition"]')
    .clear()
    .type(condition);

  cy.get('input[aria-label="Rank"]')
    .clear()
    .type(rank);
};

export const addNewLabelToGroup = (
  groupName,
  labelName,
  condition,
  rank
) => {

  // Find tab with the group name and click on it
  cy.get('button[data-baseweb="tab"')
    .should("exist")
    .find('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(groupName)
    .click();

  buttonCheckExists("Add label to group");
  buttonClick("Add label to group");

  cy.get('input[aria-label="Label Name"]')
    .clear()
    .type(labelName);

  cy.get('textarea[aria-label="Condition"]')
    .clear()
    .type(condition);

  cy.get('input[aria-label="Rank"]')
    .clear()
    .type(rank);

  buttonCheckExists("Create");
  buttonClick("Create");

  // Find tab with the group name and click on it
  cy.get('div[data-baseweb="tab-list"]')
    .should("exist")
    .contains(groupName)
    .should("exist")
    .click();

  // Validate that newly created label is found on the page
  cy.get('section[tabindex="0"]')
    .find('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(labelName)
    .should("exist");

};

// For ungrouped label, specify "Ungrouped" in the groupName argument
export const labelDelete = (
  groupName,
  labelName
) => {

  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(labelName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {  // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        .contains('🗑️')
        .should("exist")
        .click()
    })
};

export function checkGroupLabelNotExist(groupName) {
  cy.log("Validate that group label does not exist",  groupName);

    cy.visit("/");

    cy.get("span")
      .contains("Labels")
      .should("be.visible")
      .click();

    cy.get("span")
      .contains("Query Labels")
      .should("be.visible")
      .find(groupName)
      .should("not.exist");

    checkNoErrorOnThePage();
};

export const fillInTheSettingsConfigForm = (
  computeCreditCost,
  serverlessCreditCost,
  storageCost
) => {

  cy.get('input[aria-label="Compute Credit Cost"]')
    .clear()
    .type(computeCreditCost);

  cy.get('input[aria-label="Serverless Credit Cost"]')
    .clear()
    .type(serverlessCreditCost);

  cy.get('input[aria-label="Storage Cost (/tb)"]')
    .clear()
    .type(storageCost);
};

export const dropDownOpen = (dropDownName) => {
  cy.get('.row-widget.stSelectbox')
    .contains(dropDownName)
    .should("exist")
    .parents('.row-widget.stSelectbox')
    .should("exist")
    .within(() => {
      cy.get('svg[title="open"]')
        .should("exist")
        .click({force: true});
    })
};

export const dropDownElementClick = (dropDownElementName) => {
  cy.get('li[role="option"]')
    .should("exist")
    .contains(dropDownElementName)
    .should("exist")
    .click();
};

export const labelUpdateClick = (
  groupName,
  labelName
) => {

  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(labelName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {  // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        .contains('✏️')
        .should("exist")
        .click()
    })
};

// labelName: which label to update
// newLabelName: values for the new label name
// newCondition: values for the new condition
// If "None" - don't modify the field
export const updateUngroupedLabelForm = (
  labelName,
  newLabelName,
  newCondition
) => {

  cy.log('Updating label: labelName', labelName);

  if ( newLabelName != "None" ) {
    cy.get('input[aria-label="Label Name"]')
      .clear()
      .type(newLabelName).type('{enter}');
  }

  if ( newCondition != "None" ) {
    cy.get('textarea[aria-label="Condition"]')
      .clear()
      .type(newCondition).type('{command+enter}');
  }
};
