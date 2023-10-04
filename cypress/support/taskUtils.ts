import * as snowflake from "snowflake-sdk";
import { QUERY_TEXT_1 } from "../e2e/Labels/utilsAndConstants/labelTestConstants";
import { PROBE_CONDITION_TEXT_1 } from "../e2e/Probes/utils/probeTestConstants";

function connectToSnowflake(config) {
  const connectionConfig = {
    account: config.env["SNOWFLAKE_ACCOUNT"],
    username: config.env["SNOWFLAKE_USERNAME"],
    password: config.env["SNOWFLAKE_PASSWORD"],
    database: config.env["OPSCENTER_DATABASE"],
  };

  const connection = snowflake.createConnection(connectionConfig);

  connection.connect(function (err, conn) {
    if (err) {
      throw new Error("~~~ Unable to connect: " + err.message);
    }
  });

  return connection;
}

// The way connection.connect works doesn't allow for a try/catch block.
// Even if there is an error, it will keep trying to connect and, eventually, throw the error
export function deleteProbes(config) {
  const connection = connectToSnowflake(config);

  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: "delete from internal.probes",
      complete: function (err, stmt, rows) {
        if (err) {
          reject("~~ Failed to delete probes:" + err.message);
        } else {
          resolve(null);
        }
      },
    });
  });
}

export function createProbe(
  config,
  probeConfig: {
    name: string;
    condition: string;
    notifyTheAuthor: boolean;
    notifyAuthorMethod: string;
    notifyOthers: boolean;
    notifyOtherMethod: string;
    cancelTheQuery: boolean;
  }
) {
  const {
    name,
    condition,
    notifyTheAuthor,
    notifyAuthorMethod,
    notifyOthers,
    notifyOtherMethod,
    cancelTheQuery,
  } = probeConfig;
  const connection = connectToSnowflake(config);

  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: `INSERT INTO INTERNAL.PROBES (name, condition, NOTIFY_WRITER, NOTIFY_WRITER_METHOD, NOTIFY_OTHER, NOTIFY_OTHER_METHOD, CANCEL, PROBE_CREATED_AT, PROBE_MODIFIED_AT) values ('${
        name || "defaultTestName"
      }', '${condition || PROBE_CONDITION_TEXT_1}', '${
        notifyTheAuthor || false
      }', '${notifyAuthorMethod || ""}', '${notifyOthers || false}','${
        notifyOtherMethod || ""
      }', '${cancelTheQuery || false}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      complete: function (err, stmt, rows) {
        if (err) {
          reject("~~ Failed to create probe:" + err.message);
        } else {
          resolve(null);
        }
      },
    });
  });
}

export function deleteLabels(config) {
  const connection = connectToSnowflake(config);

  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: "delete from internal.labels",
      complete: function (err, stmt, rows) {
        if (err) {
          reject("~~ Failed to delete labels:" + err.message);
        } else {
          resolve(null);
        }
      },
    });
  });
}

export function createLabel(
  config,
  labelConfig: {
    name?: string;
    condition: string;
    groupRank?: number;
    groupName?: string;
    isDynamic?: boolean;
  }
) {
  const { name, condition, groupRank, groupName, isDynamic } = labelConfig;
  const connection = connectToSnowflake(config);

  return groupName && !isDynamic
    ? new Promise((resolve, reject) => {
        connection.execute({
          sqlText: `INSERT INTO INTERNAL.LABELS (name, condition, group_rank, group_name, is_dynamic, label_created_at, label_modified_at) values ('${
            name || "defaultLabelName"
          }', '${condition || QUERY_TEXT_1}', ${
            groupRank || 100
          }, '${groupName}', ${
            isDynamic || false
          }, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          complete: function (err, stmt, rows) {
            if (err) {
              reject("~~ Failed to create label:" + err.message);
            } else {
              resolve(null);
            }
          },
        });
      })
    : groupName && isDynamic
    ? new Promise((resolve, reject) => {
        connection.execute({
          sqlText: `INSERT INTO INTERNAL.LABELS (group_name, condition, is_dynamic, label_created_at, label_modified_at) values ('${groupName}', '${
            condition || QUERY_TEXT_1
          }', ${isDynamic || false}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          complete: function (err, stmt, rows) {
            if (err) {
              reject("~~ Failed to create label:" + err.message);
            } else {
              resolve(null);
            }
          },
        });
      })
    : new Promise((resolve, reject) => {
        connection.execute({
          sqlText: `INSERT INTO INTERNAL.LABELS (name, condition, is_dynamic, label_created_at, label_modified_at) values ('${
            name || "defaultLabelName"
          }', '${
            condition || QUERY_TEXT_1
          }', 'false', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          complete: function (err, stmt, rows) {
            if (err) {
              reject("~~ Failed to create label:" + err.message);
            } else {
              resolve(null);
            }
          },
        });
      });
}
