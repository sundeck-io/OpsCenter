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
  noLog?: boolean;
}) => {
  const { clickElem, contains, forceClick, noLog } = options;
  cy.get(clickElem, { log: !noLog })
    .should("exist")
    .as(`clickElem-${clickElem}`);
  if (contains) {
    cy.get(`@clickElem-${clickElem}`, { log: !noLog })
      .contains(contains, { log: !noLog })
      .as(`clickElem-${contains}`);
    cy.get(`@clickElem-${contains}`, { log: !noLog }).click({
      log: !noLog,
      force: forceClick,
    });
  } else {
    cy.get(`@clickElem-${clickElem}`, { log: !noLog })
      .scrollIntoView()
      .should("be.visible")
      .click({
        log: !noLog,
        force: forceClick,
      });
  }
  checkNoErrorOnThePage();
};
