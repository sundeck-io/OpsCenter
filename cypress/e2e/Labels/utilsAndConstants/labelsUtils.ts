import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import {
  clickCheck,
  buttonCheckExists,
  buttonClick,
} from "../../../support/clickUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { UNGROUPED } from "./labelTestConstants";

export const fillInNewUngroupedLabelForm = (options: {
  labelName: string;
  condition: string;
}) => {
  const { labelName, condition } = options;
  if (labelName) {
    cy.get('input[aria-label="Label Name"]').clear().type(labelName);
  }

  if (condition) {
    cy.get('textarea[aria-label="Condition"]')
      .clear()
      .type(condition)
      .type("{command+enter}");
  }
};

export const fillInNewLabelForm = (options: {
  groupName?: string;
  labelName: string;
  condition: string;
  rank?: string;
}) => {
  const { groupName, labelName, condition, rank } = options;

  cy.get('input[aria-label="Label Name"]').clear().type(labelName);

  groupName &&
    groupName !== UNGROUPED &&
    cy.get('input[aria-label="Group Name"]').clear().type(groupName);

  rank &&
    cy.get('input[aria-label="Group Rank"]').clear().type(rank).type("{enter}");

  cy.get('textarea[aria-label="Condition"]')
    .clear()
    .type(condition)
    .type("{command+enter}");
};

export const addNewLabelToGroup = (options: {
  groupName: string;
  labelName: string;
  condition: string;
  rank?: string;
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

  rank && cy.get('input[aria-label="Group Rank"]').clear().type(rank);

  cy.get('textarea[aria-label="Condition"]').clear().type(condition);

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

export const labelDelete = (options: {
  groupName: string; // For ungrouped label, specify "Ungrouped" in the groupName argument
  labelName: string;
}) => {
  const { groupName, labelName } = options;

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
};

export function checkLabelExists(options: {
  labelName: string;
  groupName: string;
  doesExist: boolean;
  indexNumber?: number;
}) {
  const { labelName, groupName, doesExist, indexNumber } = options;

  clickCheck({
    clickElem: '[data-testid="stMarkdownContainer"]',
    contains: groupName,
  });

  cy.dataId({ value: "stMarkdownContainer" })
    .contains(labelName)
    .should(doesExist ? "exist" : "not.exist");
}

export function checkUpdatedLabelExists(options: {
  labelName: string;
  groupName?: string;
  condition?: string;
  rank?: string;
  doesExist: boolean;
}) {
  const { labelName, groupName, condition, rank, doesExist } = options;

  clickCheck({
    clickElem: '[data-testid="stMarkdownContainer"]',
    contains: groupName ? groupName : UNGROUPED,
  });

  cy.dataId({ value: "stHorizontalBlock" })
    .should("exist")
    .contains(labelName)
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {
      condition &&
        cy
          .dataId({ value: "stVerticalBlock" })
          .contains(condition)
          .should("exist");

      rank &&
        cy.dataId({ value: "stVerticalBlock" }).contains(rank).should("exist");
    });
}

export const labelUpdateClick = (labelName: string) => {
  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(labelName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
    .should("exist")
    .within(() => {
      // Only searches within specific stHorizontalBlock that has probeName
      clickCheck({
        clickElem: 'div[data-testid="column"]',
        contains: "✏️",
      });
    });
};

export const updateLabelForm = (options: {
  newLabelName?: string;
  newCondition?: string;
  newRank?: string;
}) => {
  const { newLabelName, newCondition, newRank } = options;

  newLabelName &&
    cy
      .get('input[aria-label="Label Name"]')
      .clear()
      .type(newLabelName)
      .type("{enter}");

  newRank &&
    cy
      .get('input[aria-label="Group Rank"]')
      .clear()
      .type(newRank)
      .type("{enter}");

  newCondition &&
    cy
      .get('textarea[aria-label="Condition"]')
      .clear()
      .type(newCondition)
      .type("{command+enter}");
};

export const checkLabelFormValues = (options: {
  labelName: string;
  condition: string;
  groupName?: string;
  rank?: string;
}) => {
  const { labelName, condition, groupName, rank } = options;
  cy.get('input[aria-label="Label Name"]').should("have.value", labelName);
  cy.get('textarea[aria-label="Condition"]').should("have.value", condition);
  groupName &&
    cy.get('input[aria-label="Group Name"]').should("have.value", groupName);
  rank && cy.get('input[aria-label="Group Rank"]').should("have.value", rank);
};

export const createNewLabel = (options: {
  // Do not use for adding to a label group. Use addNewLabelToGroup() instead.
  labelName: string;
  condition: string;
  groupName?: string;
  rank?: string;
}) => {
  const { labelName, condition, groupName, rank } = options;
  buttonClick(groupName ? "New (in group)" : "New");
  fillInNewLabelForm({
    labelName: labelName,
    groupName: groupName,
    condition: condition,
    rank: rank,
  });
  buttonClick("Create");
  checkNoErrorOnThePage();
  checkOnCorrectPage({
    headerText: "Query Labels",
    notRightPageText: ["New Label", "Edit Label"],
    notRightPageButton: "Cancel",
  });
  checkLabelExists({
    labelName: labelName,
    groupName: groupName ? groupName : UNGROUPED,
    doesExist: true,
  });
};

export const deleteLabel = (options: {
  labelName: string;
  groupName?: string;
}) => {
  const { labelName, groupName } = options;
  checkOnCorrectPage({
    headerText: "Query Labels",
    notRightPageText: ["New Label", "Edit Label"],
    notRightPageButton: "Cancel",
  });
  checkNoErrorOnThePage();

  labelDelete({
    groupName: groupName ? groupName : UNGROUPED,
    labelName: labelName,
  });

  checkLabelExists({
    labelName: labelName,
    groupName: groupName ? groupName : UNGROUPED,
    doesExist: false,
  });
};
