import { checkNoErrorOnThePage } from "../../support/alertUtils";
import { checkInitialLoading } from "../../support/loadingUtils";
import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";
import { BUTTON_TEXT, HEADER_TEXT } from "../../support/testConstants";
import { LabelsButtonTests } from "./tests/labelsButtonsTests";
import { MultipleLabelsCRUDTests } from "./tests/multipleLabelsCRUDTests";
import { SingleDynamicGroupedLabelCRUDTests } from "./tests/singleDynamicGroupedLabelCRUDTests";
import { SingleGroupedLabelCRUDTests } from "./tests/singleGroupedLabelCRUDTests";
import { SingleUngroupedLabelCRUDTests } from "./tests/singleUngroupedLabelCRUDTests";

describe("Labels section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/Labels");

    checkInitialLoading();
    checkNoErrorOnThePage();

    checkOnCorrectPage({
      headerText: HEADER_TEXT.LABELS,
      notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
      notRightPageButton: BUTTON_TEXT.CANCEL,
    });
  });

  LabelsButtonTests();
  SingleUngroupedLabelCRUDTests();
  SingleGroupedLabelCRUDTests();
  SingleDynamicGroupedLabelCRUDTests();
  MultipleLabelsCRUDTests();
});
