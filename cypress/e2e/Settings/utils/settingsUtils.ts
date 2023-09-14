export const fillInTheSettingsConfigForm = (options: {
  computeCreditCost: string;
  serverlessCreditCost: string;
  storageCost: string;
}) => {
  const { computeCreditCost, serverlessCreditCost, storageCost } = options;
  cy.get('input[aria-label="Compute Credit Cost"]')
    .clear()
    .type(computeCreditCost);

  cy.get('input[aria-label="Serverless Credit Cost"]')
    .clear()
    .type(serverlessCreditCost);

  cy.get('input[aria-label="Storage Cost (/tb)"]').clear().type(storageCost);
};
