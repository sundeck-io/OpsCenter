export function setup() {
  Cypress.config("baseUrl", Cypress.env("OPSCENTER_URL"));
}
