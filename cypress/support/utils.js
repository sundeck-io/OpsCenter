export function setup() {
  Cypress.config("baseUrl", Cypress.env("OPSCENTER_URL"));
}

export function checkNoErrorOnThePage() {
  // Validate no exception is thrown
  cy.get('div[class="stException"]').should("not.exist");
};
