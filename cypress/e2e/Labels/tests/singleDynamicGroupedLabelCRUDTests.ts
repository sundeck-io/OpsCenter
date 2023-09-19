import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { setup } from "../../../support/setupUtils";
import {
  BUTTON_TEXT,
  DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
  DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2,
  LABEL_TYPES,
} from "../utilsAndConstants/labelTestConstants";
import {
  createNewLabel,
  deleteLabel,
  labelUpdateClick,
  updateLabelForm,
  checkLabelFormValues,
  checkUpdatedLabelExists,
} from "../utilsAndConstants/labelsUtils";

export const SingleDynamicGroupedLabelCRUDTests = () =>
  describe("Single Dynamic Grouped Label Tests", () => {
    before(() => {
      setup();
    });

    describe("Able to Create / Read / Update / Delete", () => {
      const groupName = generateUniqueName("initialDynamicGroupedCRUD");

      it("Create dynamic grouped label group", () => {
        createNewLabel({
          groupName: groupName,
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
          type: LABEL_TYPES.DYNAMIC_GROUPED,
        });
      });

      it("Read / Update an dynamic grouped label (can fail if previous test in this section failed)", () => {
        labelUpdateClick({
          groupName: groupName,
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
        });
        checkLabelFormValues({
          groupName: groupName,
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
        });

        updateLabelForm({
          newCondition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2,
        });

        buttonClick(BUTTON_TEXT.UPDATE);

        checkUpdatedLabelExists({
          groupName: groupName,
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2,
          doesExist: true,
        });
      });

      it("Delete dynamic grouped label (can fail if a previous test in this section failed)", () => {
        deleteLabel({
          groupName: groupName,
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2,
        });
      });
    });
  });
