import { LABEL_FORM_FIELDS, UNGROUPED } from "./labelTestConstants";

export const checkPresenceOfGroupNameInput = (options: {
  isPresent: boolean;
}) => {
  const { isPresent = true } = options;
  cy.dataId("stMarkdownContainer")
    .contains(LABEL_FORM_FIELDS.GROUP_NAME)
    .should(($el) => {
      if (isPresent) {
        expect($el).to.exist;
      } else {
        expect($el).to.not.exist;
      }
    });
};

export const fillInNewLabelForm = (options: {
  groupName?: string;
  labelName?: string;
  condition: string;
  rank?: string;
}) => {
  const { groupName, labelName, condition, rank } = options;

  labelName && cy.get('input[aria-label="Label Name"]').clear().type(labelName);

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
  labelName?: string;
  condition: string;
  groupName?: string;
  rank?: string;
}) => {
  const { labelName, condition, groupName, rank } = options;
  labelName &&
    cy.get('input[aria-label="Label Name"]').should("have.value", labelName);
  cy.get('textarea[aria-label="Condition"]').should("have.value", condition);
  groupName &&
    cy.get('input[aria-label="Group Name"]').should("have.value", groupName);
  rank && cy.get('input[aria-label="Group Rank"]').should("have.value", rank);
};
