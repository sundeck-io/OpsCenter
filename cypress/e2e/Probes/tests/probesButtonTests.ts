import { buttonClick } from "../../../support/clickUtils";
import { checkOnCorrectPage } from "../../../support/pageAssertionUtils";
import { BUTTON_TEXT, HEADER_TEXT } from "../../../support/testConstants";

export const ProbesButtonTests = () => {
  describe("Probes Button Tests", () => {
    it("New button takes you to the probes form", () => {
      buttonClick(BUTTON_TEXT.NEW);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_PROBE });
    });
    it("Cancel button takes you back from the probes form", () => {
      buttonClick(BUTTON_TEXT.NEW);
      checkOnCorrectPage({ headerText: HEADER_TEXT.CREATE_PROBE });
    });
  });
};
