import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import {
  clickCheck,
  buttonCheckExists,
  buttonClick,
} from "../../../support/clickUtils";
import { checkForLoading } from "../../../support/loadingUtils";

// If "None" - don't modify the field
export const fillInNewUngroupedLabelForm = (options: {
  labelName: string;
  condition: string;
}) => {
  const { labelName, condition } = options;
  if (labelName != "None") {
    cy.get('input[aria-label="Label Name"]').clear().type(labelName);
  }

  if (condition != "None") {
    cy.get('textarea[aria-label="Condition"]')
      .clear()
      .type(condition)
      .type("{command+enter}");
  }
};

export const fillInNewGroupedLabelForm = (options: {
  groupName: string;
  labelName: string;
  condition: string;
  rank: string;
}) => {
  const { groupName, labelName, condition, rank } = options;
  cy.get('input[aria-label="Group Name"]').clear().type(groupName);

  cy.get('input[aria-label="Label Name"]').clear().type(labelName);

  cy.get('textarea[aria-label="Condition"]').clear().type(condition);

  cy.get('input[aria-label="Rank"]').clear().type(rank);
};

export const addNewLabelToGroup = (options: {
  groupName: string;
  labelName: string;
  condition: string;
  rank: string;
}) => {
  const { groupName, labelName, condition, rank } = options;
  // Find tab with the group name and click on it
  cy.get('button[data-baseweb="tab"')
    .should("exist")
    .find('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(groupName)
    .as("labelGroupTab");

  clickCheck({ clickElem: "@labelGroupTab" });

  buttonCheckExists("Add label to group");
  buttonClick("Add label to group");

  cy.get('input[aria-label="Label Name"]').clear().type(labelName);

  cy.get('textarea[aria-label="Condition"]').clear().type(condition);

  cy.get('input[aria-label="Rank"]').clear().type(rank);

  buttonCheckExists("Create");
  buttonClick("Create");

  // Find tab with the group name and click on it

  cy.get('div[data-baseweb="tab-list"]')
    .scrollIntoView()
    .should("be.visible")
    .as("tabs");

  clickCheck({ clickElem: "@tabs", contains: groupName });

  // Validate that newly created label is found on the page
  cy.get('section[tabindex="0"]')
    .find('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(labelName)
    .should("exist");
};

// For ungrouped label, specify "Ungrouped" in the groupName argument
export const labelDelete = (options: {
  groupName: string;
  labelName: string;
}) => {
  const { groupName, labelName } = options;
  cy.log("*** labelDelete (begin): groupName:labelName", groupName, labelName);
  checkForLoading();

  // Find tab with the group name and click on it
  cy.get('div[data-baseweb="tab-list"]')
    .scrollIntoView()
    .should("be.visible")
    .as("tabs");

  clickCheck({ clickElem: "@tabs", contains: groupName, forceClick: true });

  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(labelName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {
      // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        .contains("🗑️")
        .should("exist")
        .click({ force: true });
    });

  cy.log("*** labelDelete (end): groupName", labelName);
};

export function checkGroupLabelNotExist(groupName: string) {
  cy.log("Validate that group label does not exist", groupName);

  cy.visit("/");

  cy.get("span").contains("Labels").should("be.visible").click();

  cy.get("span")
    .contains("Query Labels")
    .should("be.visible")
    .find(groupName)
    .should("not.exist");

  checkNoErrorOnThePage();
}

export const labelUpdateClick = (labelName: string) => {
  cy.log("Updating label: labelName", labelName);
  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(labelName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {
      // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        .contains("✏️")
        .should("exist")
        .click();
    });
};

// labelName: which label to update
// newLabelName: values for the new label name
// newCondition: values for the new condition
// If "None" - don't modify the field
export const updateUngroupedLabelForm = (options: {
  labelName: string;
  newLabelName: string;
  newCondition: string;
}) => {
  const { labelName, newLabelName, newCondition } = options;
  cy.log("Updating label: labelName", labelName);

  if (newLabelName != "None") {
    cy.get('input[aria-label="Label Name"]')
      .clear()
      .type(newLabelName)
      .type("{enter}");
  }

  if (newCondition != "None") {
    cy.get('textarea[aria-label="Condition"]')
      .clear()
      .type(newCondition)
      .type("{command+enter}");
  }
};
