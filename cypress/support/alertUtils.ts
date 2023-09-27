import { checkForLoading, checkInitialLoading } from "./loadingUtils";
import { ERROR_ALLOW_LIST_RELOAD } from "./errorAllowListConstants";

export function checkNoErrorOnThePage() {
  checkInitialLoading();
  checkForLoading();

  // Validate no exception is thrown
  cy.get('div[class="stException"]', { log: false })
    // stops the test from throwing it's own error so we can retrieve the internal text
    .should(() => {
      return;
    })
    // allows us to grab the internal text of the error
    .then(($errorElem) => {
      if ($errorElem.length > 0) {
        const reloaded = ERROR_ALLOW_LIST_RELOAD.filter((errorPhase) => {
          if ($errorElem.text().includes(errorPhase)) {
            cy.log("Error allowlist reload phrase reached: " + errorPhase);
            return true;
          }
        });
        if (reloaded.length === 0) {
          throw new Error(`Error on screen: ${$errorElem.text()}`);
        } else {
          cy.reloadWait();
        }
      }
    });

  cy.get('[role="alert"]', { log: false })
    // stops the test from throwing it's own error so we can retrieve the internal text
    .should(() => {
      return;
    })
    // allows us to grab the internal text of the error
    .then(($errorElem) => {
      if ($errorElem.length > 0) {
        cy.log(`Alert on screen: ${$errorElem.text()}`);
      }
    });
}

// Check for Success notification with particular text presence
export function checkSuccessAlert(notificationText: string) {
  cy.get('div[role="alert"][data-baseweb="notification"]', {
    timeout: 60000,
  })
    .and("contain", notificationText)
    .should("exist");
}

// Check for failure notification with particular text presence
export function checkFailureAlert(notificationText: string) {
  cy.get('div[role="alert"][data-baseweb="notification"]', {
    timeout: 60000,
  }).and("contain", notificationText);
}
