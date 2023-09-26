import { checkForLoading, checkInitialLoading } from "./loadingUtils";

export {};

declare global {
  namespace Cypress {
    interface Chainable {
      /**
       * Custom command to select DOM element by data-cy attribute.
       * @example cy.dataCy('greeting')
       */
      dataCy(value: string, timeout?: number): Chainable<JQuery<HTMLElement>>;
      dataId(value: string, timeout?: number): Chainable<JQuery<HTMLElement>>;
      dataBW(value: string, timeout?: number): Chainable<JQuery<HTMLElement>>;
      reloadWait(): void;
    }
  }
}

Cypress.Commands.add("dataCy", (value, timeout) => {
  return cy.get(`[data-cy=${value}]`, {
    timeout: timeout ? timeout : 10000,
  });
});

Cypress.Commands.add("dataId", (value, timeout) => {
  return cy.get(`[data-testid=${value}]`, {
    timeout: timeout ? timeout : 10000,
  });
});

Cypress.Commands.add("dataBW", (value, timeout) => {
  return cy.get(`[data-baseweb=${value}]`, {
    timeout: timeout ? timeout : 10000,
  });
});

Cypress.Commands.add("reloadWait", () => {
  cy.reload().then(() => {
    checkInitialLoading();
    checkForLoading();
  });
});
