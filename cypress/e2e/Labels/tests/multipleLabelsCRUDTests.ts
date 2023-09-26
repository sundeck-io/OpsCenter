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
      const label_1 = generateUniqueName("firstLabelUngrouped");
      const label_2 = generateUniqueName("secondLabelUngrouped");
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
      /*
      SUSANNAH FIX THIS
      index-57eefd73.js:121139 AssertionError: Timed out retrying after 40000ms: Expected to find content: 'crudMultipleGroupedLabels_679455bd-28da-40cd-85c9-df195af06047' within the element: <div.st-dd.st-b3.st-de.st-df.st-dg.st-dh.st-di.st-dj.st-dk.st-dl.st-dm.st-dn.st-do> but never did.

Because this error occurred during a `after each` hook we are skipping the remaining tests in the current suite: `Able to Create / Delete a g...`
    at clickCheck (webpack://opscenter/./cypress/support/clickUtils.ts:31:0)
    at labelDelete (webpack://opscenter/./cypress/e2e/Labels/utilsAndConstants/labelsUtils.ts:41:14)
    at Context.eval (webpack://opscenter/./cypress/e2e/Labels/tests/multipleLabelsCRUDTests.ts:98:27)
      */
      const groupName = generateUniqueName("crudMultipleGroupedLabels");
      const label_1 = generateUniqueName("firstLabelGrouped");
      const label_2 = generateUniqueName("secondLabelGrouped");
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

        for (let i = 0; i < labelList.length; i++) {
          labelDelete({
            groupName,
            labelName: labelList[i],
          });
          if (i !== labelList.length - 2) {
            cy.log("Checking that label does not exist");
            checkLabelExists({
              labelName: labelList[i],
              groupName: groupName,
              doesExist: false,
            });
          }
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
