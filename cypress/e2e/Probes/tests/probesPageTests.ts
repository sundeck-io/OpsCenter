import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { clickUpdateActionButton } from "../../../support/listingPageUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { BUTTON_TEXT } from "../../../support/testConstants";
import {
  PROBE_CONDITION_TEXT_1,
  PROBE_CONDITION_TEXT_2,
  PROBE_EMAIL_OTHERS,
  PROBE_SLACK_OTHERS,
} from "../utils/probeTestConstants";
import {
  checkProbeFormValues,
  fillInOrUpdateProbeForm,
} from "../utils/probesFormUtils";
import {
  checkProbeAndValuesExistInProbesList,
  deleteProbe,
} from "../utils/probesUtils";

export const ProbesCRUDTests = () => {
  describe("Single Probe Tests", () => {
    describe("Able to Create", () => {
      const probe_1 = generateUniqueName("probeCRUDTests");

      it("Create probe", () => {
        buttonClick(BUTTON_TEXT.NEW);
        fillInOrUpdateProbeForm({
          probeName: probe_1,
          condition: PROBE_CONDITION_TEXT_1,
          notifyTheAuthor: true,
          cancelTheQuery: true,
          notifyOthers: PROBE_EMAIL_OTHERS,
        });
        buttonClick(BUTTON_TEXT.CREATE);
        checkOnCorrectPage({ headerText: "Query Probes" });
        checkProbeAndValuesExistInProbesList({
          doesExist: true,
          probeName: probe_1,
          condition: PROBE_CONDITION_TEXT_1,
          notifyTheAuthor: true,
          cancelTheQuery: true,
          notifyOthers: PROBE_EMAIL_OTHERS,
        });
      });
    });

    describe("Able to Read / Update / Delete", () => {
      const probe_1 = generateUniqueName("probeCRUDTests");
      const probe_2 = generateUniqueName("updatedProbeCRUDTests");
      beforeEach(() => {
        cy.snowflakeSql("createProbe", {
          taskConfig: {
            name: probe_1,
            condition: PROBE_CONDITION_TEXT_1,
            notifyTheAuthor: true,
            cancelTheQuery: true,
            notifyOthers: PROBE_EMAIL_OTHERS,
          },
          reload: true,
        });
      });

      it("Read / Update probe", () => {
        clickUpdateActionButton({ name: probe_1 });
        checkProbeFormValues({
          probeName: probe_1,
          condition: PROBE_CONDITION_TEXT_1,
          notifyTheAuthor: true,
          cancelTheQuery: true,
          notifyOthers: PROBE_EMAIL_OTHERS,
        });
        fillInOrUpdateProbeForm({
          probeName: probe_2,
          condition: PROBE_CONDITION_TEXT_2,
          notifyTheAuthor: true,
          cancelTheQuery: true,
          notifyOthers: PROBE_SLACK_OTHERS,
        });
        buttonClick(BUTTON_TEXT.UPDATE);
        checkOnCorrectPage({ headerText: "Query Probes" });
        checkProbeAndValuesExistInProbesList({
          doesExist: true,
          probeName: probe_2,
          condition: PROBE_CONDITION_TEXT_2,
          notifyTheAuthor: false,
          cancelTheQuery: false,
          notifyOthers: PROBE_SLACK_OTHERS,
        });
      });

      it("Delete probe", () => {
        deleteProbe([probe_1]);
      });
    });
  });
};
