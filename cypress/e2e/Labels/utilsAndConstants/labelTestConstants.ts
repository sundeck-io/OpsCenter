export const UNGROUPED = "Ungrouped";

export const BUTTON_TEXT = {
  CREATE: "Create",
  UPDATE: "Update",
  CANCEL: "Cancel",
  NEW: "New",
  NEW_GROUPED: "New (in group)",
  NEW_DYNAMIC_GROUPED: "New dynamic grouped labels",
};

export const MENU_TEXT = {
  LABELS: "Labels",
  HOME: "Home",
  SETTINGS: "Settings",
  WAREHOUSES: "Warehouses",
  QUERIES: "Queries",
  PROBES: "Probes",
  LABS: "Labs",
};

export const LABEL_FORM_FIELDS = {
  LABEL_NAME: "Label Name",
  GROUP_NAME: "Group Name",
  CONDITION: "Condition",
  GROUP_RANK: "Group Rank",
};

export enum LABEL_TYPES {
  GROUPED = "GROUPED",
  UNGROUPED = "UNGROUPED",
  DYNAMIC_GROUPED = "DYNAMIC_GROUPED",
}

export const HEADER_TEXT = {
  LABELS: "Query Labels",
  HOME: "Welcome To Sundeck OpsCenter",
  CREATE_LABEL: "New Label",
  UPDATE_LABEL: "Edit Label",
};

export const QUERY_TEXT_1 = "compilation_time > 5000";
export const QUERY_TEXT_2 = "query_type = 'select'";

// for other avail text, check https://docs.snowflake.com/en/sql-reference/account-usage/query_history
export const DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1 = "QUERY_TYPE";
export const DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2 = "RELEASE_VERSION";
