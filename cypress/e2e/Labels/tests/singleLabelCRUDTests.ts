import { buttonClick } from "../../../support/clickUtils";
import { generateUniqueName } from "../../../support/formUtils";
import { setup } from "../../../support/setupUtils";
import {
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

export const SingleLabelCRUDTests = () =>
  describe("Labels section", () => {
    before(() => {
      setup();
    });

    describe("Able to Create / Delete", () => {
      describe("An ungrouped label", () => {
        const label_1 = generateUniqueName("label");

        it("Create ungrouped label", () => {
          createNewLabel({
            labelName: label_1,
            condition: QUERY_TEXT_1,
          });
        });

        it("Delete ungrouped label", () => {
          deleteLabel({
            labelName: label_1,
          });
        });
      });

      describe("An grouped label", () => {
        const label_1 = generateUniqueName("label");
        const groupName = generateUniqueName("createGroup");

        it("Create grouped label", () => {
          createNewLabel({
            labelName: label_1,
            groupName: groupName,
            condition: QUERY_TEXT_2,
            rank: "100",
          });
        });

        it("Delete grouped label", () => {
          deleteLabel({
            labelName: label_1,
            groupName: groupName,
          });
        });
      });
    });

    describe("Able to Read / Update", () => {
      describe("An ungrouped label", () => {
        const label_1 = generateUniqueName("firstLabel");
        const label_2 = generateUniqueName("newLabel");
        beforeEach(() => {
          createNewLabel({
            labelName: label_1,
            condition: QUERY_TEXT_1,
          });
        });

        afterEach(() => {
          deleteLabel({
            labelName: label_2,
          });
        });

        it("Update an ungrouped label", () => {
          labelUpdateClick(label_1);
          checkLabelFormValues({
            labelName: label_1,
            condition: QUERY_TEXT_1,
          });

          updateLabelForm({
            newLabelName: label_2,
            newCondition: QUERY_TEXT_2,
          });

          buttonClick("Update");

          checkUpdatedLabelExists({
            labelName: label_2,
            condition: QUERY_TEXT_2,
            doesExist: true,
          });
        });
      });

      describe("A grouped label", () => {
        const label_1 = generateUniqueName("firstLabel");
        const label_2 = generateUniqueName("newLabel");
        const groupName = generateUniqueName("updateGroup");
        beforeEach(() => {
          createNewLabel({
            labelName: label_1,
            condition: QUERY_TEXT_1,
            groupName: groupName,
            rank: "250",
          });
        });

        afterEach(() => {
          deleteLabel({
            labelName: label_2,
            groupName: groupName,
          });
        });

        it("Update a grouped label", () => {
          labelUpdateClick(label_1);

          checkLabelFormValues({
            labelName: label_1,
            condition: QUERY_TEXT_1,
            groupName: groupName,
            rank: "250",
          });

          updateLabelForm({
            newLabelName: label_2,
            newCondition: QUERY_TEXT_2,
            newRank: "500",
          });

          buttonClick("Update");

          checkUpdatedLabelExists({
            labelName: label_2,
            condition: QUERY_TEXT_2,
            rank: "500",
            groupName: groupName,
            doesExist: true,
          });
        });
      });
    });
  });
