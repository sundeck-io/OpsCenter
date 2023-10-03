import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { clickUpdateActionButton } from "../../../support/listingPageUtils";
import { BUTTON_TEXT } from "../../../support/testConstants";
import {
  DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
  DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2,
  LABEL_TYPES,
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

export const SingleDynamicGroupedLabelCRUDTests = () =>
  describe("Single Dynamic Grouped Label Tests", () => {
    const groupNameC = generateUniqueName("initialDynamicGroupedCRUD");

    it("Create dynamic grouped label group", () => {
      createNewLabel({
        groupName: groupNameC,
        condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
        type: LABEL_TYPES.DYNAMIC_GROUPED,
      });
    });

    describe("Able to Read / Update / Delete", () => {
      const groupName = generateUniqueName("initialDynamicGroupedCRUD");

      beforeEach(() => {
        cy.snowflakeSql("createLabel", {
          taskConfig: {
            groupName: groupName,
            condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
            isDynamic: true,
          },
          reload: true,
        });
      });

      it("Read / Update an dynamic grouped label (can fail if previous test in this section failed)", () => {
        clickUpdateActionButton({
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
          condition: DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1,
        });
      });
    });
  });
