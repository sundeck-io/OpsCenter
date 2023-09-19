import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { setup } from "../../../support/setupUtils";
import {
  BUTTON_TEXT,
  LABEL_TYPES,
  QUERY_TEXT_1,
  QUERY_TEXT_2,
} from "../utilsAndConstants/labelTestConstants";
import {
  createNewLabel,
  deleteLabel,
  labelUpdateClick,
  updateLabelForm,
  checkLabelFormValues,
  checkUpdatedLabelExists,
} from "../utilsAndConstants/labelsUtils";

export const SingleUngroupedLabelCRUDTests = () =>
  describe("Single Ungrouped Label Tests", () => {
    before(() => {
      setup();
    });

    describe("Able to Create / Read / Update / Delete", () => {
      const label_1 = generateUniqueName("initialUnGroupedLabelCRUD");
      const label_2 = generateUniqueName("newLabelCRUD");

      it("Create ungrouped label", () => {
        createNewLabel({
          labelName: label_1,
          condition: QUERY_TEXT_1,
          type: LABEL_TYPES.UNGROUPED,
        });
      });

      it("Update an ungrouped label (can fail if previous test in this section failed)", () => {
        labelUpdateClick({ labelName: label_1 });
        checkLabelFormValues({
          labelName: label_1,
          condition: QUERY_TEXT_1,
        });

        updateLabelForm({
          newLabelName: label_2,
          newCondition: QUERY_TEXT_2,
        });

        buttonClick(BUTTON_TEXT.UPDATE);

        checkUpdatedLabelExists({
          labelName: label_2,
          condition: QUERY_TEXT_2,
          doesExist: true,
        });
      });

      it("Delete ungrouped label (can fail if a previous test in this section failed)", () => {
        deleteLabel({
          labelName: label_2,
        });
      });
    });
  });
