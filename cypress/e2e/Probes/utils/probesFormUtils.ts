import { PROBE_FORM_FIELDS } from "./probeTestConstants";

export const fillInOrUpdateProbeForm = (options: {
  probeName?: string;
  condition?: string;
  notifyTheAuthor?: boolean;
  cancelTheQuery?: boolean;
  notifyOthers?: string;
}) => {
  const {
    probeName,
    condition,
    notifyTheAuthor,
    cancelTheQuery,
    notifyOthers,
  } = options;

  probeName &&
    cy
      .get(`input[aria-label="${PROBE_FORM_FIELDS.PROBE_NAME}"]`)
      .clear()
      .type(probeName);

  condition &&
    cy
      .get(`textarea[aria-label="${PROBE_FORM_FIELDS.CONDITION}"]`)
      .clear()
      .type(condition);

  // check({force: true}) - explanation below
  // https://docs.cypress.io/guides/references/error-messages#cy-failed-because-the-element-cannot-be-interacted-with
  notifyTheAuthor !== undefined &&
    cy
      .get(`input[aria-label="${PROBE_FORM_FIELDS.NOTIFY_AUTHOR}"]`)
      .should("exist")
      .click({ force: true });

  cancelTheQuery !== undefined &&
    cy
      .get(`input[aria-label="${PROBE_FORM_FIELDS.CANCEL_THE_QUERY}"]`)
      .should("exist")
      .click({ force: true });

  notifyOthers &&
    cy
      .get(`textarea[aria-label="${PROBE_FORM_FIELDS.NOTIFY_OTHERS}"]`)
      .clear()
      .type(notifyOthers)
      .type("{command+enter}");
};

export const checkProbeFormValues = (options: {
  probeName?: string;
  condition?: string;
  notifyTheAuthor?: boolean;
  cancelTheQuery?: boolean;
  notifyOthers?: string;
}) => {
  const {
    probeName,
    condition,
    notifyTheAuthor,
    cancelTheQuery,
    notifyOthers,
  } = options;

  probeName &&
    cy
      .get(`input[aria-label="${PROBE_FORM_FIELDS.PROBE_NAME}"]`)
      .should("exist")
      .should("have.value", probeName);

  condition &&
    cy
      .get(`textarea[aria-label="${PROBE_FORM_FIELDS.CONDITION}"]`)
      .should("exist")
      .should("have.value", condition);

  notifyTheAuthor !== undefined &&
    cy
      .get(
        `input[aria-label="${PROBE_FORM_FIELDS.NOTIFY_AUTHOR}"][aria-checked="${
          notifyTheAuthor ? true : false
        }"]`
      )
      .should("exist");

  cancelTheQuery !== undefined &&
    cy
      .get(
        `input[aria-label="${
          PROBE_FORM_FIELDS.CANCEL_THE_QUERY
        }"][aria-checked="${cancelTheQuery ? true : false}"]`
      )
      .should("exist");

  notifyOthers &&
    cy
      .get(`textarea[aria-label="${PROBE_FORM_FIELDS.NOTIFY_OTHERS}"]`)
      .should("exist")
      .should("have.value", notifyOthers);
};
