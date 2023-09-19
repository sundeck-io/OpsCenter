import { clickCheck } from "./clickUtils";

export const checkOnCorrectPage = (options: {
  headerText: string;
  notRightPageText?: string[];
  notRightPageButton?: string;
}) => {
  const { headerText, notRightPageText, notRightPageButton } = options;
  if (notRightPageText) {
    for (let i = 0; i < notRightPageText.length; i++) {
      cy.get("h1", { log: false })
        .contains(notRightPageText[i], { log: false })
        .should(($elem) => {
          return $elem;
        })
        .then(($elem) => {
          if ($elem.length > 0) {
            cy.log(
              `On the wrong page. Clicking the ${notRightPageButton} button.`
            );
            clickCheck({
              clickElem: 'button[kind="secondary"]',
              contains: notRightPageButton,
              forceClick: true,
            });
          }
        });
    }
  }
  cy.get("h1")
    .contains(headerText)
    .should("exist")
    .scrollIntoView()
    .should("be.visible");
};
