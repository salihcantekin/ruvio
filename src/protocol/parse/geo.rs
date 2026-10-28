use super::*;

pub(in crate::protocol) fn parse_geo_add<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "GEOADD expects a key and at least one longitude, latitude, and member",
        ));
    }

    let mut options = SortedSetAddOptions::default();
    let mut ttl = None;
    let mut index = 1;

    while index < args.len() {
        let token = args.bytes(index);

        if ttl.is_none()
            && let Some(found) = create_ttl_option(args, index)?
        {
            ttl = Some(found);
            index += 2;

            continue;
        }

        if token.eq_ignore_ascii_case(b"nx") {
            if options.condition == SortedSetCondition::Exists {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::NotExists;
        } else if token.eq_ignore_ascii_case(b"xx") {
            if options.condition == SortedSetCondition::NotExists {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::Exists;
        } else if token.eq_ignore_ascii_case(b"ch") {
            options.changed = true;
        } else {
            break;
        }

        index += 1;
    }

    let rest = args.len() - index;

    if rest == 0 || !rest.is_multiple_of(3) {
        return Err(ProtocolError::Invalid(
            "GEOADD expects longitude, latitude, and member triples",
        ));
    }

    let mut members = Vec::with_capacity(rest / 3);

    for at in (index..args.len()).step_by(3) {
        members.push((
            parse_float(args.bytes(at))?,
            parse_float(args.bytes(at + 1))?,
            args.arg(at + 2),
        ));
    }

    Ok(with_create_ttl(
        ttl,
        Command::GeoAdd {
            key: args.arg(0),
            members,
            options,
        },
    ))
}

pub(in crate::protocol) fn parse_geo_dist<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() != 3 && args.len() != 4 {
        return Err(ProtocolError::Invalid(
            "GEODIST expects a key, two members, and an optional unit",
        ));
    }

    let unit = if args.len() == 4 {
        parse_geo_unit(args.bytes(3))?
    } else {
        1.0
    };

    Ok(Command::GeoDist {
        key: args.arg(0),
        from: args.arg(1),
        to: args.arg(2),
        unit,
    })
}

pub(in crate::protocol) fn parse_geo_pos<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "GEOPOS expects a key and at least one member",
        ));
    }

    Ok(Command::GeoPos {
        key: args.arg(0),
        members: args.rest(1),
    })
}

pub(in crate::protocol) fn parse_geo_hash<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() < 2 {
        return Err(ProtocolError::Invalid(
            "GEOHASH expects a key and at least one member",
        ));
    }

    Ok(Command::GeoHash {
        key: args.arg(0),
        members: args.rest(1),
    })
}

pub(in crate::protocol) fn parse_geo_radius<'a>(
    args: &Args<'a, '_>,
    by_member: bool,
) -> Parsed<'a> {
    let minimum = if by_member { 4 } else { 5 };

    if args.len() < minimum {
        return Err(ProtocolError::Invalid(
            "GEORADIUS expects a key, center, radius, and unit",
        ));
    }

    let (center, radius_at) = if by_member {
        (GeoCenter::Member(args.arg(1)), 2)
    } else {
        let longitude = parse_float(args.bytes(1))?;
        let latitude = parse_float(args.bytes(2))?;

        (GeoCenter::Coordinates(longitude, latitude), 3)
    };

    let radius = parse_float(args.bytes(radius_at))?;
    let unit = parse_geo_unit(args.bytes(radius_at + 1))?;

    if radius < 0.0 {
        return Err(ProtocolError::Invalid("radius must be non-negative"));
    }

    let options = parse_geo_options(args, radius_at + 2, true)?;

    Ok(Command::GeoSearch {
        key: args.arg(0),
        center,
        area: GeoArea::Radius(radius * unit),
        unit,
        descending: options.descending,
        ordered: options.ordered,
        count: options.count,
        any: options.any,
        with_dist: options.with_dist,
        with_hash: options.with_hash,
        with_coord: options.with_coord,
        store: options.store,
        query: if by_member {
            GeoCommand::RadiusByMember
        } else {
            GeoCommand::Radius
        },
    })
}

pub(in crate::protocol) fn parse_geo_search<'a>(args: &Args<'a, '_>, store: bool) -> Parsed<'a> {
    if args.len() < if store { 2 } else { 1 } {
        return Err(ProtocolError::Invalid(
            "GEOSEARCH expects a key, a center, and a shape",
        ));
    }

    let destination = if store { Some(args.arg(0)) } else { None };
    let key_at = usize::from(store);
    let key = args.arg(key_at);
    let mut center = None;
    let mut area = None;
    let mut unit = 1.0;
    let mut descending = false;
    let mut saw_order = false;
    let mut count = None;
    let mut any = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut with_coord = false;
    let mut storedist = false;
    let mut index = key_at + 1;

    while index < args.len() {
        let token = args.bytes(index);

        if token.eq_ignore_ascii_case(b"frommember") {
            if center.is_some() || index + 1 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            center = Some(GeoCenter::Member(args.arg(index + 1)));
            index += 1;
        } else if token.eq_ignore_ascii_case(b"fromlonlat") {
            if center.is_some() || index + 2 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let longitude = parse_float(args.bytes(index + 1))?;
            let latitude = parse_float(args.bytes(index + 2))?;

            center = Some(GeoCenter::Coordinates(longitude, latitude));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"byradius") {
            if area.is_some() || index + 2 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let radius = parse_float(args.bytes(index + 1))?;

            unit = parse_geo_unit(args.bytes(index + 2))?;

            if radius < 0.0 {
                return Err(ProtocolError::Invalid("radius must be non-negative"));
            }

            area = Some(GeoArea::Radius(radius * unit));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"bybox") {
            if area.is_some() || index + 3 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let width = parse_float(args.bytes(index + 1))?;
            let height = parse_float(args.bytes(index + 2))?;

            unit = parse_geo_unit(args.bytes(index + 3))?;

            if width < 0.0 || height < 0.0 {
                return Err(ProtocolError::Invalid(
                    "box dimensions must be non-negative",
                ));
            }

            area = Some(GeoArea::Box {
                width_m: width * unit,
                height_m: height * unit,
            });

            index += 3;
        } else if token.eq_ignore_ascii_case(b"withdist") {
            with_dist = true;
        } else if token.eq_ignore_ascii_case(b"withhash") {
            with_hash = true;
        } else if token.eq_ignore_ascii_case(b"withcoord") {
            with_coord = true;
        } else if token.eq_ignore_ascii_case(b"asc") {
            descending = false;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"desc") {
            descending = true;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"any") {
            any = true;
        } else if token.eq_ignore_ascii_case(b"count") {
            if index + 1 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let value = parse_usize(args.bytes(index + 1))?;

            if value == 0 {
                return Err(ProtocolError::Invalid("COUNT must be > 0"));
            }

            count = Some(value);
            index += 1;
        } else if token.eq_ignore_ascii_case(b"storedist") && store {
            storedist = true;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    let Some(center) = center else {
        return Err(ProtocolError::Invalid(
            "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
        ));
    };

    let Some(area) = area else {
        return Err(ProtocolError::Invalid(
            "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
        ));
    };

    if any && count.is_none() {
        return Err(ProtocolError::Invalid(
            "the ANY argument requires COUNT argument",
        ));
    }

    if store && (with_dist || with_hash || with_coord) {
        return Err(ProtocolError::Invalid(
            "GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ));
    }

    Ok(Command::GeoSearch {
        key,
        center,
        area,
        unit,
        descending,
        ordered: saw_order || (count.is_some() && !any),
        count,
        any,
        with_dist,
        with_hash,
        with_coord,
        store: destination.map(|destination| GeoStore {
            destination,
            distance: storedist,
        }),
        query: if store {
            GeoCommand::SearchStore
        } else {
            GeoCommand::Search
        },
    })
}

pub(in crate::protocol) struct GeoOptions<'a> {
    pub(in crate::protocol) descending: bool,
    pub(in crate::protocol) ordered: bool,
    pub(in crate::protocol) count: Option<usize>,
    pub(in crate::protocol) any: bool,
    pub(in crate::protocol) with_dist: bool,
    pub(in crate::protocol) with_hash: bool,
    pub(in crate::protocol) with_coord: bool,
    pub(in crate::protocol) store: Option<GeoStore<'a>>,
}

/// `GEORADIUS` options from argument `from` on.
pub(in crate::protocol) fn parse_geo_options<'a>(
    args: &Args<'a, '_>,
    from: usize,
    allow_store: bool,
) -> Result<GeoOptions<'a>, ProtocolError> {
    let mut descending = false;
    let mut saw_order = false;
    let mut count = None;
    let mut any = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut with_coord = false;
    let mut store = None;
    let mut index = from;

    while index < args.len() {
        let token = args.bytes(index);

        if token.eq_ignore_ascii_case(b"withdist") {
            with_dist = true;
        } else if token.eq_ignore_ascii_case(b"withhash") {
            with_hash = true;
        } else if token.eq_ignore_ascii_case(b"withcoord") {
            with_coord = true;
        } else if token.eq_ignore_ascii_case(b"asc") {
            descending = false;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"desc") {
            descending = true;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"any") {
            any = true;
        } else if token.eq_ignore_ascii_case(b"count") {
            if index + 1 >= args.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let value = parse_usize(args.bytes(index + 1))?;

            if value == 0 {
                return Err(ProtocolError::Invalid("COUNT must be > 0"));
            }

            count = Some(value);
            index += 1;
        } else if token.eq_ignore_ascii_case(b"storedist") && allow_store {
            if index + 1 >= args.len() || store.is_some() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            store = Some(GeoStore {
                destination: args.arg(index + 1),
                distance: true,
            });

            index += 1;
        } else if token.eq_ignore_ascii_case(b"store") && allow_store {
            if index + 1 >= args.len() || store.is_some() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            store = Some(GeoStore {
                destination: args.arg(index + 1),
                distance: false,
            });

            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    if any && count.is_none() {
        return Err(ProtocolError::Invalid(
            "the ANY argument requires COUNT argument",
        ));
    }

    if store.is_some() && (with_dist || with_hash || with_coord) {
        return Err(ProtocolError::Invalid(
            "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ));
    }

    let ordered = saw_order || (count.is_some() && !any);

    Ok(GeoOptions {
        descending,
        ordered,
        count,
        any,
        with_dist,
        with_hash,
        with_coord,
        store,
    })
}

pub(in crate::protocol) fn parse_geo_unit(bytes: &[u8]) -> Result<f64, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"m") {
        return Ok(1.0);
    }

    if bytes.eq_ignore_ascii_case(b"km") {
        return Ok(1_000.0);
    }

    if bytes.eq_ignore_ascii_case(b"ft") {
        return Ok(0.3048);
    }

    if bytes.eq_ignore_ascii_case(b"mi") {
        return Ok(1_609.34);
    }

    Err(ProtocolError::Invalid(
        "unsupported unit provided. please use M, KM, FT, MI",
    ))
}
