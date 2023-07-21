import connection


def get_sundeck_url():
    return Config.get("url")


def set_sundeck_url(url: str):
    Config.set("url", url)


def refresh():
    Config.refresh()


def clear_cache():
    Config.clear_cache()


def up_to_date():
    return str(Config.get("post_setup")) == "v1"


def has_sundeck():
    return has_tenant_url() or Config.get("url") is not None


def has_tenant_url():
    return Config.get("tenant_url") is not None


def get_tenant_url():
    return Config.get("tenant_url")


def setup_complete():
    return has_sundeck() and up_to_date()


def set_costs(credit, serverless, tb):
    Config.set("compute_credit_cost", credit)
    Config.set("serverless_credit_cost", serverless)
    Config.set("storage_cost", tb)


def dval(value, dval):
    return value if value is not None else dval


def get_costs():
    return (
        dval(Config.get("compute_credit_cost"), 2.00),
        dval(Config.get("serverless_credit_cost"), 3.00),
        dval(Config.get("storage_cost"), 40.00),
    )


def get_materialization_complete():
    return (
        Config.get("WAREHOUSE_EVENTS_MAINTENANCE") is not None
        and Config.get("QUERY_HISTORY_MAINTENANCE") is not None
    )


def get_compute_credit_cost():
    return Config.get("compute_credit_cost") or 2


class Config:
    _props = None

    @classmethod
    def refresh(cls):
        config = connection.execute_select("""select * from internal.config""")
        if not config.empty:
            props = dict(zip(config["KEY"], config["VALUE"]))
            cls._props = props
        else:
            props = {}
        return props

    @classmethod
    def _get(cls) -> dict:
        if cls._props is not None:
            return cls._props

        return cls.refresh()

    @classmethod
    def clear_cache(cls):
        cls._props = None

    @classmethod
    def get(cls, key: str):
        return cls._get().get(key)

    @classmethod
    def set(cls, key: str, value):
        sql = """
MERGE INTO internal.config AS target
USING (
  SELECT %(key)s AS key, %(value)s::string AS value
) AS source
ON target.key = source.key
WHEN MATCHED THEN
  UPDATE SET value = source.value
WHEN NOT MATCHED THEN
  INSERT (key, value)
  VALUES (source.key, source.value);
        """
        connection.execute(sql, {"key": key, "value": value})
        refresh()
