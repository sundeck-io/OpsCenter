export const checkForLoading = () => {
  // presently, the for loop needs to be outside the then() to work
  for (let i = 0; i < 5; i++) {
    cy.get('[data-testid="stStatusWidget"]', { log: false })
      // the .should() here is error buffering so we don't get an error if this doesn't exist
      .should(($el) => {
        return $el; // not actually necessary, but is when is returned regardless so put here for readability
      })
      .then(($el) => {
        if ($el.length === 0) {
          cy.log("No loading spinner found, waiting 500ms and trying again");
          cy.wait(500, { log: false });
        }
      });
  }

  // if we still have a loading spinner, wait for it to disappear
  cy.get('[data-testid="stStatusWidget"]', {
    timeout: 440000,
  }).should("not.exist");
};

export const checkInitialLoading = () => {
  // In initial load, the page will say "Please wait...".We need to wait for that
  // to disappear before we check for the running spinner to disappear as well.
  // After the initial load, this will be a split second since it will not exist
  // and move on.
  cy.get('[data-testid="stMarkdownContainer"]', {
    timeout: 240000,
    log: false,
  }).as("pleaseWaitContainer");
  cy.get("@pleaseWaitContainer")
    .contains("Please wait...", { log: false })
    .should(($el) => {
      if ($el.length > 0) {
        throw new Error("Please wait... screen timed out");
      }
    });
};
