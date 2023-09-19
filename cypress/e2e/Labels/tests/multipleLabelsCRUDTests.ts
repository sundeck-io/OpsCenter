import { checkNoErrorOnThePage } from "../../../support/alertUtils";
import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { setup } from "../../../support/setupUtils";
import {
  QUERY_TEXT_1,
  QUERY_TEXT_2,
  UNGROUPED,
} from "../utilsAndConstants/labelTestConstants";
import {
  fillInNewLabelForm,
  addNewLabelToGroup,
  labelDelete,
  checkLabelExists,
} from "../utilsAndConstants/labelsUtils";

export const MultipleLabelsCRUDTests = () =>
  describe("Multiple label Creation/Deletion tests", () => {
    before(() => {
      setup();
    });

    describe("Able to Create / Delete an ungrouped label with multiple labels", () => {
      const label_1 = generateUniqueName("firstLabel");
      const label_2 = generateUniqueName("secondLabel");
      const labelList = [label_1, label_2];

      afterEach(() => {
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
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
        buttonClick("New");
        fillInNewLabelForm({
          labelName: label_1,
          groupName: UNGROUPED,
          condition: QUERY_TEXT_1,
        });
        buttonClick("Create");
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
        });
        checkLabelExists({
          labelName: label_1,
          groupName: UNGROUPED,
          doesExist: true,
        });

        buttonClick("New");
        fillInNewLabelForm({
          labelName: label_2,
          condition: QUERY_TEXT_2,
        });
        buttonClick("Create");
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
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
      const labelList = [label_1, label_2];

      afterEach(() => {
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
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
        buttonClick("New (in group)");
        fillInNewLabelForm({
          labelName: label_1,
          groupName: groupName,
          condition: QUERY_TEXT_1,
          rank: "100",
        });
        buttonClick("Create");
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
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
          rank: "200",
        });
        checkNoErrorOnThePage();
        checkOnCorrectPage({
          headerText: "Query Labels",
          notRightPageText: ["New Label", "Edit Label"],
          notRightPageButton: "Cancel",
        });
        checkLabelExists({
          labelName: label_2,
          groupName: groupName,
          doesExist: true,
        });
      });
    });
  });
