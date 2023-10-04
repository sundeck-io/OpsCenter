import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { clickUpdateActionButton } from "../../../support/listingPageUtils";
import { BUTTON_TEXT } from "../../../support/testConstants";
import {
  LABEL_TYPES,
  QUERY_TEXT_1,
  QUERY_TEXT_2,
} from "../utilsAndConstants/labelTestConstants";
import {
  checkLabelFormValues,
  updateLabelForm,
} from "../utilsAndConstants/labelsFormUtils";
import {
  createNewLabel,
  deleteLabel,
  checkUpdatedLabelExists,
} from "../utilsAndConstants/labelsUtils";

export const SingleGroupedLabelCRUDTests = () =>
  describe("Single Grouped Label Tests", () => {
    describe("Able to Create / Read / Update/ Delete", () => {
      const label_1 = generateUniqueName("initialGroupedLabelCRUD");
      const label_2 = generateUniqueName("newLabelReadUpdate");
      const groupName = generateUniqueName("createSingleGroupCRUD");
      const rank = "100";
      const newRank = "500";

      it("Create grouped label", () => {
        createNewLabel({
          labelName: label_1,
          groupName: groupName,
          condition: QUERY_TEXT_1,
          rank: rank,
          type: LABEL_TYPES.GROUPED,
        });
      });

      it("Read / Update a grouped label (can fail if previous test in this section failed)", () => {
        clickUpdateActionButton({ name: label_1, groupName: groupName });

        checkLabelFormValues({
          labelName: label_1,
          condition: QUERY_TEXT_1,
          groupName: groupName,
          rank: rank,
        });

        updateLabelForm({
          newLabelName: label_2,
          newCondition: QUERY_TEXT_2,
          newRank: newRank,
        });

        buttonClick(BUTTON_TEXT.UPDATE);

        checkUpdatedLabelExists({
          labelName: label_2,
          condition: QUERY_TEXT_2,
          rank: newRank,
          groupName: groupName,
          doesExist: true,
        });
      });

      it("Delete grouped label (can fail if a previous test in this section failed)", () => {
        deleteLabel({
          labelName: label_2,
          groupName: groupName,
        });
      });
    });
  });
