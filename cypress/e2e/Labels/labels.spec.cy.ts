import { checkNoErrorOnThePage } from "../../support/alertUtils";
import { clickCheck } from "../../support/clickUtils";
import { checkForLoading } from "../../support/loadingUtils";
import { checkOnCorrectPage } from "../../support/pageAssertionUtils";
import { setup } from "../../support/setupUtils";
import { LabelsButtonTests } from "./tests/labelsButtonsTests";
import { MultipleLabelsCRUDTests } from "./tests/multipleLabelsCRUDTests";
import { SingleLabelCRUDTests } from "./tests/singleLabelCRUDTests";

describe("Labels section", () => {
  before(() => {
    setup();
  });

  beforeEach(() => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: "Labels" });

    checkOnCorrectPage({
      headerText: "Query Labels",
      notRightPageText: ["New Label", "Edit Label"],
      notRightPageButton: "Cancel",
    });
    checkNoErrorOnThePage();
  });

  LabelsButtonTests();
  SingleLabelCRUDTests();
  // MultipleLabelsCRUDTests();
});
