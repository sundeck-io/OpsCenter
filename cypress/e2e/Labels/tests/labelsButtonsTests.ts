import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import {
  clickCheck,
  buttonClick,
  buttonCheckExists,
} from "../../../support/clickUtils";
import { checkForLoading } from "../../../support/loadingUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { setup } from "../../../support/setupUtils";
import { checkPresenceOfGroupNameInput } from "../utilsAndConstants/labelsFormUtils";

export const LabelsButtonTests = () =>
  describe("Label Buttons Tests", () => {
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

    it("Labels Buttons: Validate that New and Cancel buttons work anddon't fail to load the page", () => {
      buttonClick("New");
      checkOnCorrectPage({ headerText: "New Label" });
      checkPresenceOfGroupNameInput({ isPresent: false });

      cy.log("~~~ Clicking Cancel button");
      buttonClick("Cancel");
      buttonCheckExists("New");
      checkOnCorrectPage({
        headerText: "Query Labels",
        notRightPageText: ["New Label", "Edit Label"],
        notRightPageButton: "Cancel",
      });
    });

    it("Labels Buttons: Validate that New (in group) and Cancel buttons work anddon't fail to load the page", () => {
      buttonClick("New (in group)");
      checkOnCorrectPage({ headerText: "New Label" });
      checkPresenceOfGroupNameInput({ isPresent: true });

      cy.log("~~~ Clicking Cancel button");
      buttonClick("Cancel");
      buttonCheckExists("New");
      checkOnCorrectPage({
        headerText: "Query Labels",
        notRightPageText: ["New Label", "Edit Label"],
        notRightPageButton: "Cancel",
      });
    });
  });
