import { v4 as uuidv4 } from "uuid";

function generateUUID() {
  const uuid = uuidv4();
  return uuid;
}

export function generateUniqueName(prefix: string) {
  const uuid = generateUUID();
  const uniqueName = `${prefix}_${uuid}`;
  return uniqueName;
}

export const dropDownOpen = (dropDownName: string) => {
  cy.get(".row-widget.stSelectbox")
    .contains(dropDownName)
    .should("exist")
    .parents(".row-widget.stSelectbox")
    .should("exist")
    .within(() => {
      cy.get('svg[title="open"]').should("exist").click();
    });
};
