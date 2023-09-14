import { checkNoErrorOnThePage } from "./alertUtils";

export const buttonClick = (buttonName: string) => {
  clickCheck({
    clickElem: 'button[kind="secondary"]',
    contains: buttonName,
    forceClick: true,
  });
};

export const buttonOnTabClick = (buttonName: string) => {
  clickCheck({
    clickElem: 'button[kind="secondaryFormSubmit"]',
    contains: buttonName,
    forceClick: true,
  });
};

export const buttonCheckExists = (buttonName: string) => {
  cy.get('button[kind="secondary"]').contains(buttonName).should("exist");
  checkNoErrorOnThePage();
};

export const dropDownElementClick = (dropDownElementName: string) => {
  clickCheck({ clickElem: 'li[role="option"]', contains: dropDownElementName });
};

export const clickCheck = (options: {
  clickElem: string;
  contains?: string;
  forceClick?: boolean;
}) => {
  cy.get(options.clickElem)
    .should("exist")
    .as(`clickElem-${options.clickElem}`);
  if (options.contains) {
    cy.get(`@clickElem-${options.clickElem}`)
      .contains(options.contains)
      .as(`clickElem-${options.contains}`);
    cy.get(`@clickElem-${options.contains}`).click(
      options.forceClick ? { force: true } : undefined
    );
  } else {
    cy.get(`@clickElem-${options.clickElem}`)
      .scrollIntoView()
      .should("be.visible")
      .click(options.forceClick ? { force: true } : undefined);
  }
  checkNoErrorOnThePage();
};
