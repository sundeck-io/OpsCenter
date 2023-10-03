import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import { buttonClick, buttonCheckExists } from "../../../support/clickUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { BUTTON_TEXT, HEADER_TEXT } from "../../../support/testConstants";
import { checkPresenceOfGroupNameInput } from "../utilsAndConstants/labelsFormUtils";

export const LabelsButtonTests = () =>
  describe("Label Buttons Tests", () => {
    it("Labels Buttons: Validate that New Label button works and doesn't fail to load the page", () => {
      buttonClick(BUTTON_TEXT.NEW);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_LABEL });
      checkPresenceOfGroupNameInput({ isPresent: false });
    });

    it("Labels Buttons: Validate that Cancel button on New Label page works and doesn't fail to load the page", () => {
      buttonClick(BUTTON_TEXT.NEW);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_LABEL });
      checkPresenceOfGroupNameInput({ isPresent: false });

      cy.log("Clicking Cancel button");
      buttonClick(BUTTON_TEXT.CANCEL);
      checkNoErrorOnThePage();
      buttonCheckExists(BUTTON_TEXT.NEW);
      checkOnCorrectPage({
        headerText: HEADER_TEXT.LABELS,
        notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
        notRightPageButton: BUTTON_TEXT.CANCEL,
      });
    });

    it("Labels Buttons: Validate that New (in group) button works and doesn't fail to load the page", () => {
      buttonClick(BUTTON_TEXT.NEW_GROUPED);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_LABEL });
      checkPresenceOfGroupNameInput({ isPresent: true });
    });

    it("Labels Buttons: Validate that Cancel button on the New (in group) page works and doesn't fail to load the page", () => {
      buttonClick(BUTTON_TEXT.NEW_GROUPED);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_LABEL });
      checkPresenceOfGroupNameInput({ isPresent: true });

      cy.log("Clicking Cancel button");
      buttonClick(BUTTON_TEXT.CANCEL);
      checkNoErrorOnThePage();
      buttonCheckExists(BUTTON_TEXT.NEW);
      checkOnCorrectPage({
        headerText: HEADER_TEXT.LABELS,
        notRightPageText: [HEADER_TEXT.CREATE_LABEL, HEADER_TEXT.UPDATE_LABEL],
        notRightPageButton: BUTTON_TEXT.CANCEL,
      });
    });
  });
