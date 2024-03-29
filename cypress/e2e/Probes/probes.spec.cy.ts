import { checkNoErrorOnThePage } from "../../support/alertUtils";
import { checkInitialLoading } from "../../support/loadingUtils";
import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";
import { BUTTON_TEXT, HEADER_TEXT } from "../../support/testConstants";
import { ProbesButtonTests } from "./tests/probesButtonTests";
import { ProbesCRUDTests } from "./tests/probesPageTests";

describe("Probes section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/Query_Monitors");

    checkInitialLoading();
    checkNoErrorOnThePage();

    checkOnCorrectPage({
      headerText: HEADER_TEXT.PROBES,
      notRightPageText: [HEADER_TEXT.CREATE_PROBE, HEADER_TEXT.UPDATE_PROBE],
      notRightPageButton: BUTTON_TEXT.CANCEL,
    });
  });

  ProbesButtonTests();
  ProbesCRUDTests();
});
