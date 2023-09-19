export const checkPresenceOfGroupNameInput = (options: {
  isPresent: boolean;
}) => {
  const { isPresent = true } = options;
  cy.dataId({ value: "stMarkdownContainer" })
    .contains("Group Name")
    .should(($el) => {
      if (isPresent) {
        expect($el).to.exist;
      } else {
        expect($el).to.not.exist;
      }
    });
};
