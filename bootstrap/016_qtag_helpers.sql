
-- turn a qtag into a map of maps
create or replace function tools.qtag_to_map(qtags variant)
returns variant
language javascript
as
$$
let ret = {};
try {
  QTAGS.forEach((qtag) => {
    if (!(qtag['SOURCE'] in ret)) {
      ret[qtag['SOURCE']] = {};
    }
    ret[qtag['SOURCE']][qtag.KEY] = qtag.VALUE
  });
  return ret;
} catch {
 return undefined;
}

$$;
-- match a qtag with a specific key, value and source, given the transformed qtag
create or replace function tools.qtag(qtag variant, source varchar, key varchar, value varchar)
returns boolean
language javascript
as
$$
try {
return QTAG[SOURCE][KEY] === VALUE;
} catch {
return false;
}
$$;
-- check if a qtag exists wihit a given key/source, given the transformed qtag
create or replace function tools.qtag_exists(qtag variant, source varchar, key varchar)
returns boolean
language javascript
as
$$
try {
return QTAG[SOURCE][KEY] !== undefined;
} catch {
return false;
}
$$;
-- extract the value for a given qtag key/source, given the transformed qtag
create or replace function tools.qtag_value(qtag variant, source varchar, key varchar)
returns varchar
language javascript
as
$$
try {
return QTAG[SOURCE][KEY];
} catch {
return undefined;
}
$$;
-- extract the keys for a given qtag source, given the transformed qtag
create or replace function tools.qtag_keys(qtag variant, source varchar)
returns variant
language javascript
as
$$
try {
return Object.keys(QTAG[SOURCE]);
} catch {
return undefined;
}
$$;
-- extract the sources for a given qtag, given the transformed qtag
create or replace function tools.qtag_sources(qtag variant)
returns variant
language javascript
as
$$
try {
return Object.keys(QTAG);
} catch {
return undefined;
}
$$;
-- match a qtag value with regex and with a specific key and source, given the transformed qtag
create or replace function tools.qtag_matches(qtag variant, source varchar, key varchar, pattern varchar, parameters varchar)
returns boolean
as
$$
coalesce(rlike(tools.qtag_value(qtag, source, key), pattern, parameters), FALSE)
$$;
-- match a qtag value with regex and with a specific key and source, given the transformed qtag
create or replace function tools.qtag_matches(qtag variant, source varchar, key varchar, pattern varchar)
returns boolean
as
$$
coalesce(rlike(tools.qtag_value(qtag, source, key), pattern), FALSE)
$$;
