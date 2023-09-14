import { checkForLoading } from "./loadingUtils";

export function checkNoErrorOnThePage() {
  checkForLoading();

  // Validate no exception is thrown
  cy.get('div[class="stException"]')
    // stops the test from throwing it's own error so we can retrieve the internal text
    .should(() => {
      return;
    })
    // allows us to grab the internal text of the error
    .then(($errorElem) => {
      if ($errorElem.length > 0) {
        throw new Error(`Error on screen: ${$errorElem.text()}`);
      }
    });
}

// Check for Success notification with particular text presence
export function checkSuccessAlert(notificationText: string) {
  cy.get('div[role="alert"][data-baseweb="notification"]', {
    timeout: 60000,
  }).and("contain", notificationText);
}

// Check for failure notification with particular text presence
export function checkFailureAlert(notificationText: string) {
  cy.get('div[role="alert"][data-baseweb="notification"]', {
    timeout: 60000,
  }).and("contain", notificationText);
}
