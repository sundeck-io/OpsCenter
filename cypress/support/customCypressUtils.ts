export {};

declare global {
  namespace Cypress {
    interface Chainable {
      /**
       * Custom command to select DOM element by data-cy attribute.
       * @example cy.dataCy('greeting')
       */
      dataCy(options: {
        value: string;
        timeout?: number;
      }): Chainable<JQuery<HTMLElement>>;
      dataId(options: {
        value: string;
        timeout?: number;
      }): Chainable<JQuery<HTMLElement>>;
    }
  }
}

Cypress.Commands.add(
  "dataCy",
  (options: { value: string; timeout?: number }) => {
    return cy.get(`[data-cy=${options.value}]`, {
      timeout: options.timeout ? options.timeout : 10000,
    });
  }
);

Cypress.Commands.add(
  "dataId",
  (options: { value: string; timeout?: number }) => {
    return cy.get(`[data-testid=${options.value}]`, {
      timeout: options.timeout ? options.timeout : 10000,
    });
  }
);
