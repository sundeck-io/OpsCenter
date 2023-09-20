import { checkNoErrorOnThePage } from "../../support/alertUtils";
import { clickCheck } from "../../support/clickUtils";
import { checkForLoading } from "../../support/loadingUtils";
import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";
import { LabelsButtonTests } from "./tests/labelsButtonsTests";
import { MultipleLabelsCRUDTests } from "./tests/multipleLabelsCRUDTests";
import { SingleDynamicGroupedLabelCRUDTests } from "./tests/singleDynamicGroupedLabelCRUDTests";
import { SingleGroupedLabelCRUDTests } from "./tests/singleGroupedLabelCRUDTests";
import { SingleUngroupedLabelCRUDTests } from "./tests/singleUngroupedLabelCRUDTests";
import {
  BUTTON_TEXT,
  HEADER_TEXT,
  MENU_TEXT,
} from "./utilsAndConstants/labelTestConstants";

describe("Labels section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: MENU_TEXT.LABELS });

    checkOnCorrectPage({
      headerText: HEADER_TEXT.LABELS,
      notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
      notRightPageButton: BUTTON_TEXT.CANCEL,
    });
    checkNoErrorOnThePage();
  });

  LabelsButtonTests();
  SingleUngroupedLabelCRUDTests();
  SingleGroupedLabelCRUDTests();
  SingleDynamicGroupedLabelCRUDTests();
  MultipleLabelsCRUDTests();
});
