import { checkSuccessAlert } from "../../../support/alertUtils";
import { clickCheck } from "../../../support/clickUtils";
import { checkForLoading } from "../../../support/loadingUtils";
import { PROBE_DELETED_NOTIFICATION_TEXT } from "./probeTestConstants";

export const checkProbeAndValuesExistInProbesList = (options: {
  doesExist: boolean;
  probeName: string;
  condition?: string;
  notifyTheAuthor?: boolean;
  cancelTheQuery?: boolean;
  notifyOthers?: string;
}) => {
  const {
    doesExist,
    probeName,
    condition,
    notifyTheAuthor,
    cancelTheQuery,
    notifyOthers,
  } = options;

  cy.dataId("stHorizontalBlock").should("exist").as("probeList");

  cy.get("@probeList")
    .contains(probeName)
    .should(doesExist ? "exist" : "not.exist");

  if (doesExist) {
    cy.get('div[data-testid="stMarkdownContainer"]')
      .should("exist")
      .contains(probeName)
      .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with probeName
      .should("exist")
      .within(() => {
        condition && cy.dataId("column").contains(condition).should("exist");

        notifyTheAuthor !== undefined &&
          cy
            .dataId("column")
            .dataBW("checkbox")
            .get(`input[aria-checked="${notifyTheAuthor ? true : false}"]`)
            .should("exist");

        cancelTheQuery !== undefined &&
          cy
            .dataId("column")
            .dataBW("checkbox")
            .get(`input[aria-checked="${cancelTheQuery ? true : false}"]`)
            .should("exist");

        notifyOthers &&
          cy.dataId("column").contains(notifyOthers).should("exist");
      });
  }
};

export const deleteProbe = (probeNameList: string[]) => {
  for (const probeName of probeNameList) {
    cy.get('div[data-testid="stHorizontalBlock"]')
      .should(($elem) => {
        return $elem;
      })
      .then(($elem) => {
        if ($elem.text().includes(probeName)) {
          cy.dataId("stMarkdownContainer")
            .should("exist")
            .contains(probeName)
            .should("exist")
            .parents('div[data-testid="stHorizontalBlock"]') // finds all the parents of the element with probeName
            .should("exist")
            .within(() => {
              // Only searches within specific stHorizontalBlock that has probeName
              clickCheck({
                clickElem: '[data-testid="stMarkdownContainer"]',
                contains: "🗑️",
                forceClick: true,
              });
            });

          checkSuccessAlert(PROBE_DELETED_NOTIFICATION_TEXT);

          // needed because there is some residue left post deleting that causes the test to fail without reloading
          cy.reload();
          checkForLoading();

          checkProbeAndValuesExistInProbesList({
            doesExist: false,
            probeName: probeName,
          });
        }
      });
  }
};
