import { checkNoErrorOnThePage } from "../../support/alertUtils";
import { clickCheck } from "../../support/clickUtils";
import { checkForLoading } from "../../support/loadingUtils";
import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";
import {
  BUTTON_TEXT,
  HEADER_TEXT,
  MENU_TEXT,
} from "../../support/testConstants";
import { ProbesButtonTests } from "./tests/probesButtonTests";
import { ProbesCRUDTests } from "./tests/probesPageTests";

describe("Labels section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: MENU_TEXT.PROBES });

    checkOnCorrectPage({
      headerText: HEADER_TEXT.PROBES,
      notRightPageText: [HEADER_TEXT.CREATE_PROBE, HEADER_TEXT.UPDATE_PROBE],
      notRightPageButton: BUTTON_TEXT.CANCEL,
    });

    checkNoErrorOnThePage();
  });

  ProbesButtonTests();
  ProbesCRUDTests();
});
