import { LABEL_FORM_FIELDS } from "./labelTestConstants";

export const checkPresenceOfGroupNameInput = (options: {
  isPresent: boolean;
}) => {
  const { isPresent = true } = options;
  cy.dataId({ value: "stMarkdownContainer" })
    .contains(LABEL_FORM_FIELDS.GROUP_NAME)
    .should(($el) => {
      if (isPresent) {
        expect($el).to.exist;
      } else {
        expect($el).to.not.exist;
      }
    });
};
