import { clickLabelGroupTab } from "../e2e/Labels/utilsAndConstants/labelsUtils";
import { clickCheck } from "./clickUtils";

export const clickUpdateActionButton = (options: {
  name?: string;
  condition?: string;
  groupName?: string;
}) => {
  const { name, condition, groupName } = options;
  if (groupName) {
    clickLabelGroupTab(groupName);
  }

  cy.get('div[data-testid="stHorizontalBlock"]')
    .should("exist")
    .contains(name ? name : condition)
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
