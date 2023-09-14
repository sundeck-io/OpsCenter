export const fillInProbeForm = (options: {
  probeName: string;
  condition: string;
  emailTheAuthor: boolean;
  cancelTheQuery: boolean;
  emailOthers: string;
}) => {
  const { probeName, condition, emailTheAuthor, cancelTheQuery, emailOthers } =
    options;
  cy.get('input[aria-label="Probe Name"]').clear().type(probeName);

  cy.get('textarea[aria-label="Condition"]').clear().type(condition);

  if (emailTheAuthor) {
    // check({force: true}) - explanation below
    // https://docs.cypress.io/guides/references/error-messages#cy-failed-because-the-element-cannot-be-interacted-with
    cy.get('input[aria-label="Notify the author"]')
      .should("exist")
      .check({ force: true });
  }

  if (cancelTheQuery) {
    cy.get('input[aria-label="Cancel the query"]')
      .should("exist")
      .check({ force: true });
  }

  cy.get('textarea[aria-label="Notify others (comma delimited)"]')
    .clear()
    .type(emailOthers);
};

export const probeDelete = (probeName: string) => {
  cy.log("Deleting probe: ", probeName);

  cy.get('div[data-testid="stMarkdownContainer"]')
    .should("exist")
    .contains(probeName)
    .should("exist")
    .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with probeName
    .should("exist")
    .within(() => {
      // Only searches within specific stHorizontalBlock that has probeName
      cy.get('div[data-testid="column"]')
        //.contains("&#x1F5D1;") // Does not work. This is unicode HTML representation of the wastebasket icon.
        .eq(-2) // Brute force solution: chose second before last column (wastebasket)
        .should("exist")
        .click();
    });
};
