export function setup() {
  Cypress.config("baseUrl", Cypress.env("OPSCENTER_URL"));

  // wait a minute to allow materialization to complete
  cy.wait(60000);
}

export function checkNoErrorOnThePage() {
  // Validate no exception is thrown
  cy.get('div[class="stException"]').should("not.exist");
};
