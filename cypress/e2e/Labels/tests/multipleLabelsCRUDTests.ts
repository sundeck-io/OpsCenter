import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { HEADER_TEXT, BUTTON_TEXT } from "../../../support/testConstants";
import {
  QUERY_TEXT_1,
  QUERY_TEXT_2,
  UNGROUPED,
} from "../utilsAndConstants/labelTestConstants";
import { fillInNewLabelForm } from "../utilsAndConstants/labelsFormUtils";
import {
  addNewLabelToGroup,
  labelDelete,
  checkLabelExists,
} from "../utilsAndConstants/labelsUtils";

export const MultipleLabelsCRUDTests = () =>
  describe("Multiple label Creation/Deletion tests", () => {
    describe("Able to Create / Delete an ungrouped label with multiple labels", () => {
      const label_1 = generateUniqueName("firstLabel");
      const label_2 = generateUniqueName("secondLabel");
      const labelList = [label_1, label_2];

      afterEach(() => {
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkNoErrorOnThePage();

        for (const label of labelList) {
          labelDelete({
            groupName: UNGROUPED,
            labelName: label,
          });
          checkLabelExists({
            labelName: label,
            groupName: UNGROUPED,
            doesExist: false,
          });
        }
      });

      it("Create multiple ungrouped labels", () => {
        buttonClick(BUTTON_TEXT.NEW);
        fillInNewLabelForm({
          labelName: label_1,
          groupName: UNGROUPED,
          condition: QUERY_TEXT_1,
        });
        buttonClick(BUTTON_TEXT.CREATE);
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkLabelExists({
          labelName: label_1,
          groupName: UNGROUPED,
          doesExist: true,
        });

        buttonClick(BUTTON_TEXT.NEW);
        fillInNewLabelForm({
          labelName: label_2,
          condition: QUERY_TEXT_2,
        });
        buttonClick(BUTTON_TEXT.CREATE);
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkLabelExists({
          labelName: label_2,
          groupName: UNGROUPED,
          doesExist: true,
        });
      });
    });

    describe("Able to Create / Delete a grouped label with multiple labels", () => {
      const groupName = generateUniqueName("crudMultipleGroupedLabels");
      const label_1 = generateUniqueName("firstLabel");
      const label_2 = generateUniqueName("secondLabel");
      const rank1 = "100";
      const rank2 = "200";
      const labelList = [label_1, label_2];

      afterEach(() => {
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkNoErrorOnThePage();

        for (const label of labelList) {
          labelDelete({
            groupName,
            labelName: label,
          });
          checkLabelExists({
            labelName: label,
            groupName: groupName,
            doesExist: false,
          });
        }
      });

      it("Create multiple grouped labels with different ranks", () => {
        buttonClick(BUTTON_TEXT.NEW_GROUPED);
        fillInNewLabelForm({
          labelName: label_1,
          groupName: groupName,
          condition: QUERY_TEXT_1,
          rank: rank1,
        });
        buttonClick(BUTTON_TEXT.CREATE);
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkLabelExists({
          labelName: label_1,
          groupName: groupName,
          doesExist: true,
        });

        addNewLabelToGroup({
          groupName,
          labelName: label_2,
          condition: QUERY_TEXT_2,
          rank: rank2,
        });
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: HEADER_TEXT.LABELS,
          notRightPageText: [
            HEADER_TEXT.CREATE_LABEL,
            HEADER_TEXT.UPDATE_LABEL,
          ],
          notRightPageButton: BUTTON_TEXT.CANCEL,
        });
        checkLabelExists({
          labelName: label_2,
          groupName: groupName,
          doesExist: true,
        });
      });
    });
  });
