import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import {
  clickCheck,
  buttonCheckExists,
  buttonClick,
} from "../../../support/clickUtils";
import { checkInitialLoading } from "../../../support/loadingUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { BUTTON_TEXT, HEADER_TEXT } from "../../../support/testConstants";
import { LABEL_TYPES, UNGROUPED } from "./labelTestConstants";
import { fillInNewLabelForm } from "./labelsFormUtils";

export const addNewLabelToGroup = (options: {
  groupName: string;
  labelName: string;
  condition: string;
  rank?: string;
}) => {
  const { groupName, labelName, condition, rank } = options;
  // Find tab with the group name and click on it
  clickLabelGroupTab(groupName);

  buttonCheckExists("Add label to group");
  buttonClick("Add label to group");

  cy.get('input[aria-label="Label Name"]').clear().type(labelName);

  rank && cy.get('input[aria-label="Group Rank"]').clear().type(rank);

  cy.get('textarea[aria-label="Condition"]')
    .clear()
    .type(condition)
    .type("{command+enter}");

  buttonCheckExists(BUTTON_TEXT.CREATE);
  buttonClick(BUTTON_TEXT.CREATE);

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
  labelName?: string; // dynamic grouped labels don't have names
  condition?: string;
}) => {
  const { groupName, labelName, condition } = options;

  // Find tab with the group name and click on it
  cy.get('div[data-baseweb="tab-list"]')
    .scrollIntoView()
    .should("be.visible")
    .as("tabs");

  clickCheck({ clickElem: "@tabs", contains: groupName, forceClick: true });

  if (labelName) {
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
  } else if (condition) {
    cy.get('div[data-testid="stHorizontalBlock"]')
      .should("exist")
      .contains(condition)
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
  }
};

export function checkLabelExists(options: {
  labelName?: string;
  condition?: string; // if no label name, as is the case for dynamic grouped labels
  groupName: string;
  doesExist: boolean;
  indexNumber?: number;
}) {
  const { labelName, groupName, doesExist, condition } = options;

  if (doesExist || groupName === UNGROUPED) {
    clickCheck({
      clickElem: '[data-testid="stMarkdownContainer"]',
      contains: groupName,
    });
  } else {
    cy.get('[data-testid="stMarkdownContainer"]', { log: false })
      .contains(groupName)
      .should("not.exist");
  }

  if (labelName) {
    cy.dataId("stMarkdownContainer")
      .contains(labelName)
      .should(doesExist ? "exist" : "not.exist");
  } else if (condition) {
    cy.dataId("stVerticalBlock")
      .contains(condition)
      .should(doesExist ? "exist" : "not.exist");
  }
}

export function checkUpdatedLabelExists(options: {
  labelName?: string;
  groupName?: string;
  condition?: string;
  rank?: string;
  doesExist: boolean;
}) {
  const { labelName, groupName, condition, rank, doesExist } = options;

  if (doesExist) {
    clickCheck({
      clickElem: '[data-testid="stMarkdownContainer"]',
      contains: groupName ? groupName : UNGROUPED,
    });
  }

  if (labelName) {
    cy.dataId("stHorizontalBlock")
      .should("exist")
      .contains(labelName)
      .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with labelName
      .should("exist")
      .within(() => {
        condition &&
          cy.dataId("stVerticalBlock").contains(condition).should("exist");

        rank && cy.dataId("stVerticalBlock").contains(rank).should("exist");
      });
  } else if (condition) {
    cy.dataId("stHorizontalBlock").should("exist").contains(condition);
  }
}

export const clickLabelGroupTab = (groupName: string) => {
  cy.get('button[data-baseweb="tab"')
    .should("exist")
    .find('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(groupName)
    .as("labelGroupTab");

  clickCheck({ clickElem: "@labelGroupTab" });
};

export const createNewLabel = (options: {
  // Do not use for adding to a label group. Use addNewLabelToGroup() instead.
  labelName?: string;
  condition: string;
  groupName?: string;
  type: keyof typeof LABEL_TYPES;
  rank?: string;
}) => {
  const { labelName, condition, groupName, rank, type } = options;
  buttonClick(
    type === LABEL_TYPES.UNGROUPED
      ? BUTTON_TEXT.NEW
      : type === LABEL_TYPES.GROUPED
      ? BUTTON_TEXT.NEW_GROUPED
      : BUTTON_TEXT.NEW_DYNAMIC_GROUPED
  );
  fillInNewLabelForm({
    labelName: labelName,
    groupName: groupName,
    condition: condition,
    rank: rank,
  });
  buttonClick(BUTTON_TEXT.CREATE);
  checkNoErrorOnThePage();
  checkOnCorrectPage({
    headerText: HEADER_TEXT.LABELS,
    notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
    notRightPageButton: BUTTON_TEXT.CANCEL,
  });
  checkLabelExists({
    labelName: labelName,
    condition: condition,
    groupName: groupName ? groupName : UNGROUPED,
    doesExist: true,
  });
};

export const deleteLabel = (options: {
  labelName?: string; // dynamic grouped labels don't have label names
  groupName?: string;
  condition?: string;
}) => {
  const { labelName, groupName, condition } = options;
  checkOnCorrectPage({
    headerText: HEADER_TEXT.LABELS,
    notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
    notRightPageButton: BUTTON_TEXT.CANCEL,
  });
  checkNoErrorOnThePage();

  labelDelete({
    groupName: groupName ? groupName : UNGROUPED,
    labelName: labelName,
    condition: condition,
  });

  checkNoErrorOnThePage();
  checkLabelExists({
    labelName: labelName,
    condition: condition,
    groupName: groupName ? groupName : UNGROUPED,
    doesExist: false,
  });
};
