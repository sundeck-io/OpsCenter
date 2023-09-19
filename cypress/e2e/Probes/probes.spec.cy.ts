import { checkNoErrorOnThePage } from "../../support/alertUtils";
import {
  clickCheck,
  buttonClick,
  buttonCheckExists,
} from "../../support/clickUtils";
import { generateUniqueName } from "../../support/formUtils";
import { checkForLoading } from "../../support/loadingUtils";
import { setup } from "../../support/setupUtils";
import {
  BUTTON_TEXT,
  MENU_TEXT,
} from "../Labels/utilsAndConstants/labelTestConstants";
import { fillInProbeForm, probeDelete } from "./utils/probesUtils";

describe("Probes section", () => {
  before(() => {
    setup();
  });

  it("Menu: Probes", () => {
    cy.visit("/");

    checkForLoading();

    clickCheck({ clickElem: "span", contains: MENU_TEXT.PROBES });

    cy.get("span").contains("Query Probes").should("be.visible");
    checkNoErrorOnThePage();

    // Test #1: validate that clicking on "New" button starts page without error
    buttonClick(BUTTON_TEXT.NEW);

    // Test #2: validate that clicking on "Cancel" brings form back to "New" button
    buttonClick(BUTTON_TEXT.CANCEL);
    buttonCheckExists(BUTTON_TEXT.NEW);
    checkNoErrorOnThePage();

    // Test #3: Fill the form with valid values and save
    buttonClick(BUTTON_TEXT.NEW);
    const probe_1 = generateUniqueName("probe");
    fillInProbeForm({
      probeName: probe_1,
      condition: "query_text='%tpch_sf100%'",
      emailTheAuthor: true,
      cancelTheQuery: true,
      emailOthers: "vicky@sundeck.io, jinfeng@sundeck.io",
    });
    buttonClick(BUTTON_TEXT.CREATE);

    cy.get("span")
      .contains("Query Probes", { timeout: 30000 })
      .scrollIntoView()
      .should("be.visible");
    checkNoErrorOnThePage();

    probeDelete(probe_1);

    // Test #4: Fill the form with valid values and save (checkboxes are unchecked)
    buttonClick(BUTTON_TEXT.NEW);
    checkNoErrorOnThePage();
    const probe_2 = generateUniqueName("probe");
    fillInProbeForm({
      probeName: probe_2,
      condition: "query_text='%tpch_sf1%'",
      emailTheAuthor: false,
      cancelTheQuery: false,
      emailOthers: "vicky@sundeck.io, jinfeng@sundeck.io",
    });
    buttonClick(BUTTON_TEXT.CREATE);

    cy.get("span")
      .contains("Query Probes", { timeout: 30000 })
      .scrollIntoView()
      .should("be.visible");
    checkNoErrorOnThePage();

    probeDelete(probe_2);

    // Among other things, "New" button should exist
    buttonCheckExists(BUTTON_TEXT.NEW);
  });
});
