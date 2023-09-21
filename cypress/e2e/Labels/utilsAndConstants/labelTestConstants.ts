export const UNGROUPED = "Ungrouped";

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

export const QUERY_TEXT_1 = "compilation_time > 5000";
export const QUERY_TEXT_2 = "query_type = 'select'";

// for other avail text, check https://docs.snowflake.com/en/sql-reference/account-usage/query_history
export const DYNAMIC_GROUPED_LABEL_QUERY_TEXT_1 = "QUERY_TYPE";
export const DYNAMIC_GROUPED_LABEL_QUERY_TEXT_2 = "RELEASE_VERSION";
